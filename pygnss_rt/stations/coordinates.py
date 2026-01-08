"""
Coordinate transformation utilities.

Provides transformations between reference frames and coordinate systems:
- ITRS to ETRS89 (and reverse)
- Cartesian to Ellipsoidal (geodetic)
- Ellipsoidal to Cartesian

Replaces Perl CRD.pm module.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Tuple

import numpy as np


# Constants
PI = math.pi
DEG2RAD = PI / 180.0
RAD2DEG = 180.0 / PI
SEC2RAD = PI / (180.0 * 3600.0)

# WGS84 ellipsoid parameters
WGS84_A = 6378137.0  # Semi-major axis (meters)
WGS84_F = 1 / 298.257223563  # Flattening
WGS84_B = WGS84_A * (1 - WGS84_F)  # Semi-minor axis
WGS84_E2 = 2 * WGS84_F - WGS84_F ** 2  # First eccentricity squared
WGS84_EP2 = (WGS84_A ** 2 - WGS84_B ** 2) / WGS84_B ** 2  # Second eccentricity squared

# IGb00 ellipsoid (used in Bernese)
IGB00_A = 6378137.0
IGB00_F = 1.0 / 298.2572221
IGB00_E = math.sqrt(2 * IGB00_F - IGB00_F ** 2)


@dataclass
class CartesianCoord:
    """Cartesian ECEF coordinates."""
    x: float
    y: float
    z: float


@dataclass
class EllipsoidalCoord:
    """Ellipsoidal (geodetic) coordinates."""
    latitude: float  # radians
    longitude: float  # radians
    height: float  # meters (ellipsoidal height)

    @property
    def lat_deg(self) -> float:
        """Latitude in degrees."""
        return self.latitude * RAD2DEG

    @property
    def lon_deg(self) -> float:
        """Longitude in degrees."""
        return self.longitude * RAD2DEG


def transform_itrs_to_etrs89(
    x: float,
    y: float,
    z: float,
    year: int,
    doy: int,
) -> Tuple[float, float, float]:
    """Transform coordinates from ITRS00 to ETRS89.

    Uses the 14-parameter Helmert transformation with time-dependent
    rotation rates.

    Args:
        x: X coordinate in ITRS (meters)
        y: Y coordinate in ITRS (meters)
        z: Z coordinate in ITRS (meters)
        year: Year of observation
        doy: Day of year

    Returns:
        Tuple of (X, Y, Z) in ETRS89 (meters)
    """
    # Time elapsed since reference epoch (1989.0)
    delta_t = (year + doy / 365.0) - 1989.0

    # Rotation matrix elements (IERS conventions)
    # Rotation rates in arcsec/year, converted to radians
    r_x = 0.000081 * delta_t * SEC2RAD
    r_y = 0.00049 * delta_t * SEC2RAD
    r_z = -0.000792 * delta_t * SEC2RAD

    # Scale factor
    s = 0.0

    # Translation vector (meters)
    t_x = 0.054
    t_y = 0.051
    t_z = -0.048

    # Build rotation matrix
    rot = np.array([
        [1 + s, -r_z, r_y],
        [r_z, 1 + s, -r_x],
        [-r_y, r_x, 1 + s],
    ])

    # Translation vector
    trans = np.array([[t_x], [t_y], [t_z]])

    # Input coordinates
    coord = np.array([[x], [y], [z]])

    # Transform: T = trans + rot @ coord
    result = trans + rot @ coord

    return (
        round(result[0, 0], 4),
        round(result[1, 0], 4),
        round(result[2, 0], 4),
    )


def transform_etrs89_to_itrs(
    x: float,
    y: float,
    z: float,
    year: int,
    doy: int,
) -> Tuple[float, float, float]:
    """Transform coordinates from ETRS89 to ITRS00.

    Inverse of transform_itrs_to_etrs89.

    Args:
        x: X coordinate in ETRS89 (meters)
        y: Y coordinate in ETRS89 (meters)
        z: Z coordinate in ETRS89 (meters)
        year: Year of observation
        doy: Day of year

    Returns:
        Tuple of (X, Y, Z) in ITRS (meters)
    """
    # Time elapsed since reference epoch (1989.0)
    delta_t = (year + doy / 365.0) - 1989.0

    # Rotation matrix elements
    r_x = 0.000081 * delta_t * SEC2RAD
    r_y = 0.00049 * delta_t * SEC2RAD
    r_z = -0.000792 * delta_t * SEC2RAD

    # Scale factor
    s = 0.0

    # Translation vector
    t_x = 0.054
    t_y = 0.051
    t_z = -0.048

    # Build rotation matrix (forward direction)
    rot = np.array([
        [1 + s, -r_z, r_y],
        [r_z, 1 + s, -r_x],
        [-r_y, r_x, 1 + s],
    ])

    # Inverse rotation matrix
    rot_inv = np.linalg.inv(rot)

    # Translation and coordinate vectors
    trans = np.array([[t_x], [t_y], [t_z]])
    coord = np.array([[x], [y], [z]])

    # Inverse transform: result = rot_inv @ (coord - trans)
    result = rot_inv @ (coord - trans)

    return (
        round(result[0, 0], 4),
        round(result[1, 0], 4),
        round(result[2, 0], 4),
    )


def cartesian_to_ellipsoidal(
    x: float,
    y: float,
    z: float,
    a: float = IGB00_A,
    e: float = IGB00_E,
) -> Tuple[float, float, float]:
    """Convert Cartesian (ECEF) to ellipsoidal coordinates.

    Uses iterative algorithm for latitude calculation.

    Args:
        x: X coordinate (meters)
        y: Y coordinate (meters)
        z: Z coordinate (meters)
        a: Semi-major axis (default: IGb00)
        e: First eccentricity (default: IGb00)

    Returns:
        Tuple of (latitude, longitude, height) where lat/lon are in radians
        and height is in meters (ellipsoidal height)
    """
    # Longitude (direct calculation)
    longitude = math.atan2(y, x)

    # Distance from Z-axis
    p = math.sqrt(x * x + y * y)

    # Initial latitude estimate
    phi = PI / 4.0

    # Iterative solution for latitude
    max_iterations = 100
    tolerance = 1e-13

    for _ in range(max_iterations):
        # Radius of curvature in prime vertical
        N = a / math.sqrt(1 - e * e * math.sin(phi) ** 2)

        # Height estimate
        h = p / math.cos(phi) - N

        # New latitude estimate
        phi_new = math.atan2(z, p * (1 - e * e * N / (N + h)))

        # Check convergence
        if abs(phi_new - phi) < tolerance:
            phi = phi_new
            break

        phi = phi_new

    # Final height calculation
    N = a / math.sqrt(1 - e * e * math.sin(phi) ** 2)
    h = p / math.cos(phi) - N

    return (phi, longitude, h)


def ellipsoidal_to_cartesian(
    lat: float,
    lon: float,
    height: float,
    a: float = IGB00_A,
    e: float = IGB00_E,
) -> Tuple[float, float, float]:
    """Convert ellipsoidal to Cartesian (ECEF) coordinates.

    Args:
        lat: Latitude in radians
        lon: Longitude in radians
        height: Ellipsoidal height in meters
        a: Semi-major axis (default: IGb00)
        e: First eccentricity (default: IGb00)

    Returns:
        Tuple of (X, Y, Z) in meters
    """
    # Radius of curvature in prime vertical
    N = a / math.sqrt(1 - e * e * math.sin(lat) ** 2)

    # Cartesian coordinates
    x = (N + height) * math.cos(lat) * math.cos(lon)
    y = (N + height) * math.cos(lat) * math.sin(lon)
    z = (N * (1 - e * e) + height) * math.sin(lat)

    return (x, y, z)


def geodetic_to_ecef(
    lat: float,
    lon: float,
    height: float,
) -> Tuple[float, float, float]:
    """Convert geodetic coordinates to ECEF.

    Args:
        lat: Latitude in degrees
        lon: Longitude in degrees
        height: Ellipsoidal height in meters

    Returns:
        Tuple of (X, Y, Z) in meters
    """
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)

    sin_lat = math.sin(lat_rad)
    cos_lat = math.cos(lat_rad)
    sin_lon = math.sin(lon_rad)
    cos_lon = math.cos(lon_rad)

    # Radius of curvature in the prime vertical
    N = WGS84_A / math.sqrt(1 - WGS84_E2 * sin_lat ** 2)

    x = (N + height) * cos_lat * cos_lon
    y = (N + height) * cos_lat * sin_lon
    z = (N * (1 - WGS84_E2) + height) * sin_lat

    return x, y, z


def ecef_to_geodetic(
    x: float,
    y: float,
    z: float,
) -> Tuple[float, float, float]:
    """Convert ECEF coordinates to geodetic.

    Uses iterative algorithm for high precision.

    Args:
        x: X coordinate in meters
        y: Y coordinate in meters
        z: Z coordinate in meters

    Returns:
        Tuple of (latitude, longitude, height) where
        lat/lon are in degrees, height in meters
    """
    # Longitude is straightforward
    lon = math.atan2(y, x)

    # Initial values for iteration
    p = math.sqrt(x ** 2 + y ** 2)
    lat = math.atan2(z, p * (1 - WGS84_E2))

    # Iterate to convergence
    for _ in range(10):
        sin_lat = math.sin(lat)
        N = WGS84_A / math.sqrt(1 - WGS84_E2 * sin_lat ** 2)
        lat_new = math.atan2(z + WGS84_E2 * N * sin_lat, p)

        if abs(lat_new - lat) < 1e-12:
            lat = lat_new
            break
        lat = lat_new

    # Calculate height
    sin_lat = math.sin(lat)
    cos_lat = math.cos(lat)
    N = WGS84_A / math.sqrt(1 - WGS84_E2 * sin_lat ** 2)

    if abs(cos_lat) > 1e-10:
        height = p / cos_lat - N
    else:
        height = abs(z) / abs(sin_lat) - N * (1 - WGS84_E2)

    return math.degrees(lat), math.degrees(lon), height


def xyz_to_llh(
    x: float,
    y: float,
    z: float,
) -> Tuple[float, float, float]:
    """Convenience function: Cartesian to geodetic (degrees, meters).

    Args:
        x: X coordinate (meters)
        y: Y coordinate (meters)
        z: Z coordinate (meters)

    Returns:
        Tuple of (latitude_deg, longitude_deg, height_m)
    """
    lat, lon, h = cartesian_to_ellipsoidal(x, y, z)
    return (lat * RAD2DEG, lon * RAD2DEG, h)


def llh_to_xyz(
    lat_deg: float,
    lon_deg: float,
    height: float,
) -> Tuple[float, float, float]:
    """Convenience function: Geodetic (degrees) to Cartesian.

    Args:
        lat_deg: Latitude in degrees
        lon_deg: Longitude in degrees
        height: Ellipsoidal height in meters

    Returns:
        Tuple of (X, Y, Z) in meters
    """
    lat = lat_deg * DEG2RAD
    lon = lon_deg * DEG2RAD
    return ellipsoidal_to_cartesian(lat, lon, height)


def calculate_distance(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """Calculate distance between two points using Haversine formula.

    Args:
        lat1: Latitude of point 1 in degrees
        lon1: Longitude of point 1 in degrees
        lat2: Latitude of point 2 in degrees
        lon2: Longitude of point 2 in degrees

    Returns:
        Distance in meters
    """
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2 +
        math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return WGS84_A * c


def great_circle_distance(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """Calculate great circle distance between two points.

    Alias for calculate_distance using Haversine formula.

    Args:
        lat1: Latitude of point 1 (degrees)
        lon1: Longitude of point 1 (degrees)
        lat2: Latitude of point 2 (degrees)
        lon2: Longitude of point 2 (degrees)

    Returns:
        Distance in meters
    """
    return calculate_distance(lat1, lon1, lat2, lon2)


class CoordinateTransformer:
    """High-level coordinate transformation class."""

    def __init__(self, reference_epoch: float = 1989.0):
        """Initialize transformer.

        Args:
            reference_epoch: Reference epoch for ITRS/ETRS transformation
        """
        self.reference_epoch = reference_epoch

    def itrs_to_etrs89(
        self,
        x: float,
        y: float,
        z: float,
        year: int,
        doy: int,
    ) -> CartesianCoord:
        """Transform ITRS to ETRS89."""
        xt, yt, zt = transform_itrs_to_etrs89(x, y, z, year, doy)
        return CartesianCoord(xt, yt, zt)

    def etrs89_to_itrs(
        self,
        x: float,
        y: float,
        z: float,
        year: int,
        doy: int,
    ) -> CartesianCoord:
        """Transform ETRS89 to ITRS."""
        xt, yt, zt = transform_etrs89_to_itrs(x, y, z, year, doy)
        return CartesianCoord(xt, yt, zt)

    def cartesian_to_geodetic(
        self,
        x: float,
        y: float,
        z: float,
    ) -> EllipsoidalCoord:
        """Convert Cartesian to geodetic coordinates."""
        lat, lon, h = cartesian_to_ellipsoidal(x, y, z)
        return EllipsoidalCoord(lat, lon, h)

    def geodetic_to_cartesian(
        self,
        lat: float,
        lon: float,
        height: float,
    ) -> CartesianCoord:
        """Convert geodetic to Cartesian coordinates.

        Args:
            lat: Latitude in radians
            lon: Longitude in radians
            height: Ellipsoidal height in meters
        """
        x, y, z = ellipsoidal_to_cartesian(lat, lon, height)
        return CartesianCoord(x, y, z)
