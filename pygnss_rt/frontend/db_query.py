"""
Database Query Operations

This module contains the QualityDatabaseQuery class with all get_* methods
for querying data from the database.
"""

from typing import Optional

from pygnss_rt.frontend.db_insert import QualityDatabaseInsert


class QualityDatabaseQuery(QualityDatabaseInsert):
    """Database query operations mixin"""

    def get_solutions(self, station_id: Optional[str] = None,
                      year: Optional[int] = None,
                      start_doy: Optional[int] = None,
                      end_doy: Optional[int] = None,
                      limit: int = 365) -> list[dict]:
        """Query PPP solutions with optional filters"""
        conn = self.connect()

        query = "SELECT * FROM ppp_solutions WHERE 1=1"
        params = []

        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)
        if year:
            query += " AND year = ?"
            params.append(year)
        if start_doy:
            query += " AND doy >= ?"
            params.append(start_doy)
        if end_doy:
            query += " AND doy <= ?"
            params.append(end_doy)

        query += " ORDER BY year DESC, doy DESC LIMIT ?"
        params.append(limit)

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_stats(self, station_id: Optional[str] = None,
                  year: Optional[int] = None,
                  limit: int = 365) -> list[dict]:
        """Query processing statistics"""
        conn = self.connect()

        query = "SELECT * FROM processing_stats WHERE 1=1"
        params = []

        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)
        if year:
            query += " AND year = ?"
            params.append(year)

        query += " ORDER BY year DESC, doy DESC LIMIT ?"
        params.append(limit)

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_daily_coords(self, station_id: str, year: int,
                         start_doy: int, end_doy: int) -> list[dict]:
        """Get daily XYZ coordinates for a station over a DOY range.

        Used by ERA5 comparison to obtain station lat/lon/height for interpolation.
        """
        conn = self.connect()

        result = conn.execute("""
            SELECT station_id, year, doy, x, y, z, lat, lon, height
            FROM ppp_solutions
            WHERE station_id = ? AND year = ? AND doy BETWEEN ? AND ?
            ORDER BY doy
        """, [station_id, year, start_doy, end_doy]).fetchdf()

        return result.to_dict('records')

    def get_ztd(self, station_id: str, year: int, doy: int) -> list[dict]:
        """Get hourly ZTD for a specific station and day"""
        conn = self.connect()

        result = conn.execute("""
            SELECT * FROM ztd_hourly
            WHERE station_id = ? AND year = ? AND doy = ?
            ORDER BY hour
        """, [station_id, year, doy]).fetchdf()

        return result.to_dict('records')

    def get_all_stations(self) -> list[str]:
        """Get list of all stations with solutions"""
        conn = self.connect()

        result = conn.execute(
            "SELECT DISTINCT station_id FROM ppp_solutions ORDER BY station_id"
        ).fetchdf()

        return result['station_id'].tolist() if len(result) > 0 else []

    def get_latest_processing(self, limit: int = 50) -> list[dict]:
        """Get most recently processed solutions"""
        conn = self.connect()

        result = conn.execute("""
            SELECT s.*, p.rms_unit_weight, p.num_observations
            FROM ppp_solutions s
            LEFT JOIN processing_stats p
                ON s.station_id = p.station_id AND s.year = p.year AND s.doy = p.doy
            ORDER BY s.created_at DESC
            LIMIT ?
        """, [limit]).fetchdf()

        return result.to_dict('records')

    def get_ambiguity(self, station_id: Optional[str] = None,
                      year: Optional[int] = None,
                      start_doy: Optional[int] = None,
                      end_doy: Optional[int] = None) -> list[dict]:
        """Query ambiguity resolution statistics"""
        conn = self.connect()

        query = "SELECT * FROM ambiguity_resolution WHERE 1=1"
        params = []

        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)
        if year:
            query += " AND year = ?"
            params.append(year)
        if start_doy:
            query += " AND doy >= ?"
            params.append(start_doy)
        if end_doy:
            query += " AND doy <= ?"
            params.append(end_doy)

        query += " ORDER BY station_id, year, doy"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_satellite_tracking(self, year: Optional[int] = None,
                               doy: Optional[int] = None,
                               constellation: Optional[str] = None) -> list[dict]:
        """Query satellite tracking statistics"""
        conn = self.connect()

        query = "SELECT * FROM satellite_tracking WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if constellation:
            query += " AND constellation = ?"
            params.append(constellation)

        query += " ORDER BY year, doy, prn"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_satellite_ambiguity_prn(self, year: Optional[int] = None,
                                    doy: Optional[int] = None,
                                    constellation: Optional[str] = None) -> list[dict]:
        """Query satellite-wise ambiguity resolution statistics"""
        conn = self.connect()

        query = "SELECT * FROM satellite_ambiguity_prn WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if constellation:
            query += " AND constellation = ?"
            params.append(constellation)

        query += " ORDER BY year, doy, prn"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_station_residuals(self, year: Optional[int] = None,
                              doy: Optional[int] = None,
                              station_id: Optional[str] = None,
                              constellation: Optional[str] = None) -> list[dict]:
        """Query station residuals"""
        conn = self.connect()

        query = "SELECT * FROM station_residuals WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)
        if constellation:
            query += " AND constellation = ?"
            params.append(constellation)

        query += " ORDER BY year, doy, station_id, prn"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_data_availability(self, year: Optional[int] = None,
                              doy: Optional[int] = None,
                              constellation: Optional[str] = None) -> list[dict]:
        """Query data availability statistics"""
        conn = self.connect()

        query = "SELECT * FROM data_availability WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if constellation:
            query += " AND constellation = ?"
            params.append(constellation)

        query += " ORDER BY year, doy, prn"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_receiver_clocks(self, year: Optional[int] = None,
                            doy: Optional[int] = None,
                            station_id: Optional[str] = None) -> list[dict]:
        """Query receiver clock estimates"""
        conn = self.connect()

        query = "SELECT * FROM receiver_clocks WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)

        query += " ORDER BY station_id, year, doy, epoch"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_tropospheric_gradients(self, year: Optional[int] = None,
                                   doy: Optional[int] = None,
                                   station_id: Optional[str] = None) -> list[dict]:
        """Query tropospheric gradients from ztd_hourly table"""
        conn = self.connect()

        query = """
            SELECT station_id, year, doy, hour, ztd, ztd_rms,
                   grad_n, grad_n_rms, grad_e, grad_e_rms,
                   SQRT(grad_n*grad_n + grad_e*grad_e) as grad_magnitude,
                   DEGREES(ATAN2(grad_e, grad_n)) as grad_azimuth
            FROM ztd_hourly
            WHERE grad_n IS NOT NULL AND grad_e IS NOT NULL
        """
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)

        query += " ORDER BY station_id, year, doy, hour"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_clock_stations(self, year: int, doy: int) -> list[str]:
        """Get list of stations with clock data for a specific day"""
        conn = self.connect()

        result = conn.execute("""
            SELECT DISTINCT station_id
            FROM receiver_clocks
            WHERE year = ? AND doy = ?
            ORDER BY station_id
        """, [year, doy]).fetchdf()

        return result['station_id'].tolist() if len(result) > 0 else []

    def get_gradient_stations(self, year: int, doy: int) -> list[str]:
        """Get list of stations with gradient data for a specific day"""
        conn = self.connect()

        result = conn.execute("""
            SELECT DISTINCT station_id
            FROM ztd_hourly
            WHERE year = ? AND doy = ? AND grad_n IS NOT NULL
            ORDER BY station_id
        """, [year, doy]).fetchdf()

        return result['station_id'].tolist() if len(result) > 0 else []

    def get_station_completeness(self, year: Optional[int] = None,
                                  doy: Optional[int] = None,
                                  station_id: Optional[str] = None,
                                  start_doy: Optional[int] = None,
                                  end_doy: Optional[int] = None) -> list[dict]:
        """Query station data completeness statistics"""
        conn = self.connect()

        query = "SELECT * FROM station_data_completeness WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)
        if start_doy:
            query += " AND doy >= ?"
            params.append(start_doy)
        if end_doy:
            query += " AND doy <= ?"
            params.append(end_doy)

        query += " ORDER BY station_id, year, doy"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_observation_quality(self, year: Optional[int] = None,
                                 doy: Optional[int] = None,
                                 station_id: Optional[str] = None,
                                 start_doy: Optional[int] = None,
                                 end_doy: Optional[int] = None) -> list[dict]:
        """Query observation quality metrics"""
        conn = self.connect()

        query = "SELECT * FROM observation_quality WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)
        if start_doy:
            query += " AND doy >= ?"
            params.append(start_doy)
        if end_doy:
            query += " AND doy <= ?"
            params.append(end_doy)

        query += " ORDER BY station_id, year, doy"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_data_gaps_timeline(self, year: int, start_doy: int, end_doy: int,
                               station_id: Optional[str] = None) -> list[dict]:
        """
        Get timeline data showing data gaps for stations.

        Returns data suitable for Gantt-style visualization showing
        when each station has valid data vs gaps.
        """
        conn = self.connect()

        query = """
            SELECT
                z.station_id,
                z.doy,
                z.hour,
                CASE WHEN z.ztd_rms < 0.1 THEN 'valid' ELSE 'gap' END as status
            FROM ztd_hourly z
            WHERE z.year = ? AND z.doy BETWEEN ? AND ?
        """
        params = [year, start_doy, end_doy]

        if station_id:
            query += " AND z.station_id = ?"
            params.append(station_id)

        query += " ORDER BY z.station_id, z.doy, z.hour"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')
