"""
Database Insert Operations

This module contains the QualityDatabaseInsert class with all insert_* methods
for adding data to the database.
"""

from typing import TYPE_CHECKING

from pygnss_rt.frontend.db_schema import QualityDatabaseSchema

if TYPE_CHECKING:
    from pygnss_rt.frontend.db_models import (
        PPPSolution, ProcessingStats, ZTDHourly, AmbiguityResolution,
        SatelliteTracking, SatelliteAmbiguityPRN, StationResidual,
        DataAvailability, ReceiverClock, StationDataCompleteness,
        ObservationQuality
    )


class QualityDatabaseInsert(QualityDatabaseSchema):
    """Database insert operations mixin"""

    def insert_solution(self, sol: 'PPPSolution'):
        """Insert or update a PPP solution"""
        conn = self.connect()

        # Use INSERT OR REPLACE for upsert behavior
        conn.execute("""
            INSERT OR REPLACE INTO ppp_solutions
                (station_id, year, doy, mjd, x, y, z, x_rms, y_rms, z_rms, lat, lon, height)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [sol.station_id, sol.year, sol.doy, sol.mjd,
              sol.x, sol.y, sol.z, sol.x_rms, sol.y_rms, sol.z_rms,
              sol.lat, sol.lon, sol.height])

    def insert_stats(self, stats: 'ProcessingStats'):
        """Insert or update processing statistics"""
        conn = self.connect()

        # Use INSERT OR REPLACE for upsert behavior
        conn.execute("""
            INSERT OR REPLACE INTO processing_stats
                (station_id, year, doy, mjd, rms_unit_weight, chi2_dof,
                 num_observations, num_parameters, dof, num_satellites,
                 ambiguity_fixed, ambiguity_total, ambiguity_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [stats.station_id, stats.year, stats.doy, stats.mjd,
              stats.rms_unit_weight, stats.chi2_dof, stats.num_observations,
              stats.num_parameters, stats.dof, stats.num_satellites,
              stats.ambiguity_fixed, stats.ambiguity_total, stats.ambiguity_rate])

    def insert_ztd(self, ztd: 'ZTDHourly'):
        """Insert or update hourly ZTD"""
        conn = self.connect()

        # Use INSERT OR REPLACE for upsert behavior
        conn.execute("""
            INSERT OR REPLACE INTO ztd_hourly
                (station_id, year, doy, hour, mjd, ztd, ztd_rms, grad_n, grad_n_rms, grad_e, grad_e_rms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [ztd.station_id, ztd.year, ztd.doy, ztd.hour, ztd.mjd,
              ztd.ztd, ztd.ztd_rms, ztd.grad_n, ztd.grad_n_rms, ztd.grad_e, ztd.grad_e_rms])

    def insert_ambiguity(self, amb: 'AmbiguityResolution'):
        """Insert or update ambiguity resolution stats"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO ambiguity_resolution
                (station_id, year, doy, receiver, wl_gps, wl_gal, wl_combined,
                 nl_gps, nl_gal, nl_combined)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [amb.station_id, amb.year, amb.doy, amb.receiver,
              amb.wl_gps, amb.wl_gal, amb.wl_combined,
              amb.nl_gps, amb.nl_gal, amb.nl_combined])

    def insert_satellite_tracking(self, sat: 'SatelliteTracking'):
        """Insert or update satellite tracking stats"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO satellite_tracking
                (year, doy, prn, constellation, obs_percent, obs_count, rms)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [sat.year, sat.doy, sat.prn, sat.constellation,
              sat.obs_percent, sat.obs_count, sat.rms])

    def insert_satellite_ambiguity_prn(self, sat: 'SatelliteAmbiguityPRN'):
        """Insert or update satellite-wise ambiguity resolution stats"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO satellite_ambiguity_prn
                (year, doy, prn, constellation, amb_total,
                 l1l2_solved, l1l2_cluster, l1l2_rel,
                 l5_solved, l5_cluster, l5_rel)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [sat.year, sat.doy, sat.prn, sat.constellation, sat.amb_total,
              sat.l1l2_solved, sat.l1l2_cluster, sat.l1l2_rel,
              sat.l5_solved, sat.l5_cluster, sat.l5_rel])

    def insert_station_residual(self, res: 'StationResidual'):
        """Insert or update station residual data"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO station_residuals
                (year, doy, station_id, prn, constellation, rms)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [res.year, res.doy, res.station_id, res.prn,
              res.constellation, res.rms])

    def insert_data_availability(self, da: 'DataAvailability'):
        """Insert or update data availability statistics"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO data_availability
                (year, doy, prn, constellation, obs_pct_before, obs_pct_after,
                 obs_count_before, obs_count_after, obs_rejected, rejection_rate,
                 rms_before, rms_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [da.year, da.doy, da.prn, da.constellation,
              da.obs_pct_before, da.obs_pct_after,
              da.obs_count_before, da.obs_count_after,
              da.obs_rejected, da.rejection_rate,
              da.rms_before, da.rms_after])

    def insert_receiver_clock(self, clk: 'ReceiverClock'):
        """Insert or update receiver clock estimate"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO receiver_clocks
                (station_id, year, doy, epoch, clock_offset, clock_sigma)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [clk.station_id, clk.year, clk.doy, clk.epoch,
              clk.clock_offset, clk.clock_sigma])

    def insert_station_completeness(self, sc: 'StationDataCompleteness'):
        """Insert or update station data completeness statistics"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO station_data_completeness
                (station_id, year, doy, expected_obs, actual_obs, rejected_obs,
                 completeness_pct, rejection_pct, first_epoch, last_epoch,
                 gap_hours, gap_count, has_full_day, quality_flag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [sc.station_id, sc.year, sc.doy, sc.expected_obs, sc.actual_obs,
              sc.rejected_obs, sc.completeness_pct, sc.rejection_pct,
              sc.first_epoch, sc.last_epoch, sc.gap_hours, sc.gap_count,
              sc.has_full_day, sc.quality_flag])

    def insert_observation_quality(self, oq: 'ObservationQuality'):
        """Insert or update observation quality metrics"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO observation_quality
                (station_id, year, doy, mp1, mp2, snr_l1, snr_l2,
                 cycle_slips, cycle_slip_rate, completeness_pct,
                 total_epochs, total_observations, mean_sats_per_epoch,
                 gps_sats, glo_sats, gal_sats, quality_level)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [oq.station_id, oq.year, oq.doy, oq.mp1, oq.mp2,
              oq.snr_l1, oq.snr_l2, oq.cycle_slips, oq.cycle_slip_rate,
              oq.completeness_pct, oq.total_epochs, oq.total_observations,
              oq.mean_sats_per_epoch, oq.gps_sats, oq.glo_sats, oq.gal_sats,
              oq.quality_level])
