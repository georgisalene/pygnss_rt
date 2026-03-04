"""
Database Analytics and Computation Operations

This module contains the QualityDatabaseAnalytics class with all compute_* and
analytical methods for deriving metrics from the database.
"""

from typing import Optional, TYPE_CHECKING

from pygnss_rt.frontend.db_query import QualityDatabaseQuery

if TYPE_CHECKING:
    from pygnss_rt.frontend.db_models import StationDataCompleteness


class QualityDatabaseAnalytics(QualityDatabaseQuery):
    """Database analytics and computation operations mixin"""

    def get_coordinate_repeatability(self, station_id: str,
                                     year: int,
                                     start_doy: int,
                                     end_doy: int) -> dict:
        """Calculate coordinate repeatability (std dev) over a period"""
        conn = self.connect()

        result = conn.execute("""
            SELECT
                station_id,
                COUNT(*) as num_days,
                AVG(x) as mean_x, STDDEV(x) as std_x,
                AVG(y) as mean_y, STDDEV(y) as std_y,
                AVG(z) as mean_z, STDDEV(z) as std_z,
                AVG(x_rms) as mean_x_rms,
                AVG(y_rms) as mean_y_rms,
                AVG(z_rms) as mean_z_rms
            FROM ppp_solutions
            WHERE station_id = ? AND year = ? AND doy BETWEEN ? AND ?
            GROUP BY station_id
        """, [station_id, year, start_doy, end_doy]).fetchdf()

        if len(result) > 0:
            return result.to_dict('records')[0]
        return {}

    def get_all_station_repeatability(self, year: int,
                                       start_doy: int,
                                       end_doy: int) -> list[dict]:
        """Calculate coordinate repeatability for all stations over a period.

        Returns daily solutions deviation from mean in mm (North, East, Up).
        """
        conn = self.connect()

        result = conn.execute("""
            WITH station_means AS (
                SELECT
                    station_id,
                    AVG(x) as mean_x, AVG(y) as mean_y, AVG(z) as mean_z,
                    COUNT(*) as num_days
                FROM ppp_solutions
                WHERE year = ? AND doy BETWEEN ? AND ?
                GROUP BY station_id
            ),
            station_deviations AS (
                SELECT
                    p.station_id,
                    p.doy,
                    p.x - m.mean_x as dx,
                    p.y - m.mean_y as dy,
                    p.z - m.mean_z as dz,
                    m.mean_x, m.mean_y, m.mean_z,
                    m.num_days
                FROM ppp_solutions p
                JOIN station_means m ON p.station_id = m.station_id
                WHERE p.year = ? AND p.doy BETWEEN ? AND ?
            )
            SELECT
                station_id,
                num_days,
                -- RMS of deviations in XYZ (mm)
                SQRT(AVG(dx * dx)) * 1000 as rms_x_mm,
                SQRT(AVG(dy * dy)) * 1000 as rms_y_mm,
                SQRT(AVG(dz * dz)) * 1000 as rms_z_mm,
                -- 3D RMS (mm)
                SQRT(AVG(dx*dx + dy*dy + dz*dz)) * 1000 as rms_3d_mm,
                mean_x, mean_y, mean_z
            FROM station_deviations
            GROUP BY station_id, num_days, mean_x, mean_y, mean_z
            ORDER BY station_id
        """, [year, start_doy, end_doy, year, start_doy, end_doy]).fetchdf()

        return result.to_dict('records')

    def get_daily_coordinate_deviations(self, year: int,
                                         start_doy: int,
                                         end_doy: int) -> list[dict]:
        """Get daily coordinate deviations from mean for all stations.

        Returns deviations in mm (dN, dE, dU) for time series plotting.
        """
        conn = self.connect()

        result = conn.execute("""
            WITH station_means AS (
                SELECT
                    station_id,
                    AVG(x) as mean_x, AVG(y) as mean_y, AVG(z) as mean_z
                FROM ppp_solutions
                WHERE year = ? AND doy BETWEEN ? AND ?
                GROUP BY station_id
            )
            SELECT
                p.station_id,
                p.year,
                p.doy,
                -- Deviations in XYZ (mm)
                (p.x - m.mean_x) * 1000 as dx_mm,
                (p.y - m.mean_y) * 1000 as dy_mm,
                (p.z - m.mean_z) * 1000 as dz_mm,
                m.mean_x, m.mean_y, m.mean_z
            FROM ppp_solutions p
            JOIN station_means m ON p.station_id = m.station_id
            WHERE p.year = ? AND p.doy BETWEEN ? AND ?
            ORDER BY p.station_id, p.doy
        """, [year, start_doy, end_doy, year, start_doy, end_doy]).fetchdf()

        return result.to_dict('records')

    def compute_station_completeness_from_ztd(self, year: int, doy: int,
                                               expected_obs_per_hour: int = 450) -> list['StationDataCompleteness']:
        """
        Compute station data completeness from ZTD hourly data.

        This estimates data completeness by checking which hours have valid ZTD estimates.
        A station with all 24 hours present has full coverage.

        Args:
            year: Year
            doy: Day of year
            expected_obs_per_hour: Expected observations per hour (~30 sats * 120 epochs @ 30s)

        Returns:
            List of StationDataCompleteness objects
        """
        # Import here to avoid circular import
        from pygnss_rt.frontend.db_models import StationDataCompleteness

        conn = self.connect()

        # Get all ZTD data for this day with validity check
        # Note: RMS threshold of 0.25m covers ~97% of valid observations
        # (typical RMS distribution: 0.05-0.2m for most stations)
        result = conn.execute("""
            SELECT
                station_id,
                hour,
                ztd_rms,
                CASE WHEN ztd_rms < 0.25 THEN 1 ELSE 0 END as valid_hour
            FROM ztd_hourly
            WHERE year = ? AND doy = ?
            ORDER BY station_id, hour
        """, [year, doy]).fetchdf()

        if len(result) == 0:
            return []

        # Get processing stats for observation counts
        stats = conn.execute("""
            SELECT station_id, num_observations
            FROM processing_stats
            WHERE year = ? AND doy = ?
        """, [year, doy]).fetchdf()

        stats_dict = {}
        if len(stats) > 0:
            stats_dict = dict(zip(stats['station_id'], stats['num_observations']))

        completeness_list = []

        for station_id in result['station_id'].unique():
            station_data = result[result['station_id'] == station_id]
            hours_present = set(station_data['hour'].tolist())
            valid_hours = station_data[station_data['valid_hour'] == 1]['hour'].tolist()

            # Calculate metrics
            actual_obs = stats_dict.get(station_id, len(valid_hours) * expected_obs_per_hour)
            expected_obs = 24 * expected_obs_per_hour  # Full day

            # Find gaps
            all_hours = set(range(24))
            missing_hours = all_hours - hours_present

            first_epoch = min(hours_present) if hours_present else None
            last_epoch = max(hours_present) if hours_present else None

            # Count gap hours and distinct gaps
            gap_hours = len(missing_hours)
            gap_count = 0
            if missing_hours:
                sorted_missing = sorted(missing_hours)
                gap_count = 1
                for i in range(1, len(sorted_missing)):
                    if sorted_missing[i] - sorted_missing[i-1] > 1:
                        gap_count += 1

            # Quality metrics
            completeness_pct = (len(valid_hours) / 24.0) * 100 if valid_hours else 0
            has_full_day = (first_epoch == 0 and last_epoch == 23 and gap_hours == 0)

            # Quality flag
            if completeness_pct >= 95:
                quality_flag = 'GOOD'
            elif completeness_pct >= 70:
                quality_flag = 'PARTIAL'
            else:
                quality_flag = 'POOR'

            completeness_list.append(StationDataCompleteness(
                station_id=station_id,
                year=year,
                doy=doy,
                expected_obs=expected_obs,
                actual_obs=actual_obs,
                rejected_obs=0,  # Will be updated from screening data if available
                completeness_pct=completeness_pct,
                rejection_pct=0.0,
                first_epoch=first_epoch,
                last_epoch=last_epoch,
                gap_hours=gap_hours,
                gap_count=gap_count,
                has_full_day=has_full_day,
                quality_flag=quality_flag
            ))

        return completeness_list

    def get_residuals_by_constellation(self, year: int, doy: int) -> list[dict]:
        """Get phase residuals RMS aggregated by constellation"""
        conn = self.connect()

        result = conn.execute("""
            SELECT
                constellation,
                COUNT(DISTINCT station_id) as num_stations,
                COUNT(DISTINCT prn) as num_satellites,
                AVG(rms) as mean_rms,
                MIN(rms) as min_rms,
                MAX(rms) as max_rms,
                STDDEV(rms) as std_rms
            FROM station_residuals
            WHERE year = ? AND doy = ?
            GROUP BY constellation
            ORDER BY constellation
        """, [year, doy]).fetchdf()

        return result.to_dict('records')

    def get_residuals_by_station_constellation(self, year: int, doy: int,
                                                station_id: Optional[str] = None) -> list[dict]:
        """Get phase residuals RMS by station and constellation"""
        conn = self.connect()

        query = """
            SELECT
                station_id,
                constellation,
                COUNT(DISTINCT prn) as num_satellites,
                AVG(rms) as mean_rms,
                MIN(rms) as min_rms,
                MAX(rms) as max_rms
            FROM station_residuals
            WHERE year = ? AND doy = ?
        """
        params = [year, doy]

        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)

        query += " GROUP BY station_id, constellation ORDER BY station_id, constellation"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    # ========== PPP-AR Quality Control Methods ==========

    def get_ar_summary_stats(self, year: int,
                             start_doy: int,
                             end_doy: int) -> dict:
        """Get summary AR statistics across all stations for a period.

        Returns overall WL/NL success rates and statistics.
        """
        conn = self.connect()

        result = conn.execute("""
            SELECT
                COUNT(*) as num_records,
                COUNT(DISTINCT station_id) as num_stations,
                -- Wide Lane stats
                AVG(wl_combined) as mean_wl_combined,
                AVG(wl_gps) as mean_wl_gps,
                AVG(wl_gal) as mean_wl_gal,
                MIN(wl_combined) as min_wl_combined,
                MAX(wl_combined) as max_wl_combined,
                STDDEV(wl_combined) as std_wl_combined,
                -- Narrow Lane stats
                AVG(nl_combined) as mean_nl_combined,
                AVG(nl_gps) as mean_nl_gps,
                AVG(nl_gal) as mean_nl_gal,
                MIN(nl_combined) as min_nl_combined,
                MAX(nl_combined) as max_nl_combined,
                STDDEV(nl_combined) as std_nl_combined,
                -- Stations with low AR rates
                SUM(CASE WHEN nl_combined < 50 THEN 1 ELSE 0 END) as low_nl_count,
                SUM(CASE WHEN wl_combined < 80 THEN 1 ELSE 0 END) as low_wl_count
            FROM ambiguity_resolution
            WHERE year = ? AND doy BETWEEN ? AND ?
        """, [year, start_doy, end_doy]).fetchdf()

        if len(result) > 0:
            return result.to_dict('records')[0]
        return {}

    def get_ar_by_station(self, year: int,
                          start_doy: int,
                          end_doy: int) -> list[dict]:
        """Get AR success rates aggregated by station over a period.

        Returns mean WL/NL rates and consistency metrics per station.
        """
        conn = self.connect()

        result = conn.execute("""
            SELECT
                station_id,
                receiver,
                COUNT(*) as num_days,
                -- Wide Lane
                AVG(wl_combined) as mean_wl,
                MIN(wl_combined) as min_wl,
                MAX(wl_combined) as max_wl,
                STDDEV(wl_combined) as std_wl,
                AVG(wl_gps) as mean_wl_gps,
                AVG(wl_gal) as mean_wl_gal,
                -- Narrow Lane
                AVG(nl_combined) as mean_nl,
                MIN(nl_combined) as min_nl,
                MAX(nl_combined) as max_nl,
                STDDEV(nl_combined) as std_nl,
                AVG(nl_gps) as mean_nl_gps,
                AVG(nl_gal) as mean_nl_gal,
                -- Quality flag
                CASE
                    WHEN AVG(nl_combined) >= 80 AND AVG(wl_combined) >= 90 THEN 'EXCELLENT'
                    WHEN AVG(nl_combined) >= 70 AND AVG(wl_combined) >= 85 THEN 'GOOD'
                    WHEN AVG(nl_combined) >= 50 AND AVG(wl_combined) >= 70 THEN 'ACCEPTABLE'
                    ELSE 'POOR'
                END as ar_quality
            FROM ambiguity_resolution
            WHERE year = ? AND doy BETWEEN ? AND ?
            GROUP BY station_id, receiver
            ORDER BY mean_nl DESC
        """, [year, start_doy, end_doy]).fetchdf()

        return result.to_dict('records')

    def get_ar_by_receiver_type(self, year: int,
                                start_doy: int,
                                end_doy: int) -> list[dict]:
        """Get AR success rates grouped by receiver type.

        Useful for identifying receiver-dependent AR performance.
        """
        conn = self.connect()

        result = conn.execute("""
            SELECT
                receiver,
                COUNT(DISTINCT station_id) as num_stations,
                COUNT(*) as num_records,
                -- Wide Lane
                AVG(wl_combined) as mean_wl,
                MIN(wl_combined) as min_wl,
                MAX(wl_combined) as max_wl,
                -- Narrow Lane
                AVG(nl_combined) as mean_nl,
                MIN(nl_combined) as min_nl,
                MAX(nl_combined) as max_nl
            FROM ambiguity_resolution
            WHERE year = ? AND doy BETWEEN ? AND ?
            GROUP BY receiver
            ORDER BY mean_nl DESC
        """, [year, start_doy, end_doy]).fetchdf()

        return result.to_dict('records')

    def get_ar_time_series(self, year: int,
                           start_doy: int,
                           end_doy: int,
                           station_id: Optional[str] = None) -> list[dict]:
        """Get AR success rate time series.

        Returns daily WL/NL rates for trend analysis.
        """
        conn = self.connect()

        query = """
            SELECT
                doy,
                station_id,
                wl_combined,
                wl_gps,
                wl_gal,
                nl_combined,
                nl_gps,
                nl_gal,
                receiver
            FROM ambiguity_resolution
            WHERE year = ? AND doy BETWEEN ? AND ?
        """
        params = [year, start_doy, end_doy]

        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)

        query += " ORDER BY doy, station_id"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_satellite_ar_summary(self, year: int,
                                 start_doy: int,
                                 end_doy: int) -> list[dict]:
        """Get satellite-level AR performance summary.

        Returns L1L2 and L5 fix rates by satellite.
        """
        conn = self.connect()

        result = conn.execute("""
            SELECT
                prn,
                constellation,
                COUNT(*) as num_days,
                AVG(amb_total) as mean_amb_total,
                AVG(l1l2_solved) as mean_l1l2_solved,
                AVG(l1l2_rel) as mean_l1l2_rate,
                MIN(l1l2_rel) as min_l1l2_rate,
                MAX(l1l2_rel) as max_l1l2_rate,
                AVG(l5_solved) as mean_l5_solved,
                AVG(l5_rel) as mean_l5_rate,
                MIN(l5_rel) as min_l5_rate,
                MAX(l5_rel) as max_l5_rate
            FROM satellite_ambiguity_prn
            WHERE year = ? AND doy BETWEEN ? AND ?
            GROUP BY prn, constellation
            ORDER BY constellation, prn
        """, [year, start_doy, end_doy]).fetchdf()

        return result.to_dict('records')

    def get_satellite_ar_by_constellation(self, year: int,
                                          start_doy: int,
                                          end_doy: int) -> list[dict]:
        """Get AR performance summary by constellation.

        Returns aggregated fix rates for GPS vs Galileo.
        """
        conn = self.connect()

        result = conn.execute("""
            SELECT
                constellation,
                COUNT(DISTINCT prn) as num_satellites,
                COUNT(*) as num_records,
                SUM(amb_total) as total_ambiguities,
                SUM(l1l2_solved) as total_l1l2_solved,
                AVG(l1l2_rel) as mean_l1l2_rate,
                MIN(l1l2_rel) as min_l1l2_rate,
                MAX(l1l2_rel) as max_l1l2_rate,
                SUM(l5_solved) as total_l5_solved,
                AVG(l5_rel) as mean_l5_rate,
                MIN(l5_rel) as min_l5_rate,
                MAX(l5_rel) as max_l5_rate
            FROM satellite_ambiguity_prn
            WHERE year = ? AND doy BETWEEN ? AND ?
            GROUP BY constellation
            ORDER BY constellation
        """, [year, start_doy, end_doy]).fetchdf()

        return result.to_dict('records')
