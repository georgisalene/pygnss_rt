#!/usr/bin/env python3
"""
Test script to verify the db_models refactoring.

This script tests that:
1. All dataclasses can be imported from db_models
2. QualityDatabase class can be imported and instantiated
3. All methods are available through the inheritance chain
"""

import sys

def test_imports():
    """Test that all imports work correctly"""
    print("Testing imports...")

    # Test dataclass imports
    try:
        from pygnss_rt.frontend.db_models import (
            PPPSolution,
            ProcessingStats,
            ZTDHourly,
            AmbiguityResolution,
            SatelliteTracking,
            SatelliteAmbiguityPRN,
            StationResidual,
            DataAvailability,
            ReceiverClock,
            StationDataCompleteness,
            ObservationQuality,
            QualityDatabase
        )
        print("✓ All dataclasses and QualityDatabase imported successfully")
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

    # Test QualityDatabase instantiation
    try:
        db = QualityDatabase(db_path=":memory:")
        print("✓ QualityDatabase instantiated successfully")
    except Exception as e:
        print(f"✗ Instantiation failed: {e}")
        return False

    # Test that methods from each mixin are available
    methods_to_check = {
        'connect': 'QualityDatabaseSchema',
        'close': 'QualityDatabaseSchema',
        'create_tables': 'QualityDatabaseSchema',
        'insert_solution': 'QualityDatabaseInsert',
        'insert_stats': 'QualityDatabaseInsert',
        'insert_ztd': 'QualityDatabaseInsert',
        'get_solutions': 'QualityDatabaseQuery',
        'get_stats': 'QualityDatabaseQuery',
        'get_all_stations': 'QualityDatabaseQuery',
        'get_coordinate_repeatability': 'QualityDatabaseAnalytics',
        'get_all_station_repeatability': 'QualityDatabaseAnalytics',
        'compute_station_completeness_from_ztd': 'QualityDatabaseAnalytics',
    }

    missing_methods = []
    for method_name, source in methods_to_check.items():
        if not hasattr(db, method_name):
            missing_methods.append(f"{method_name} (from {source})")

    if missing_methods:
        print(f"✗ Missing methods: {', '.join(missing_methods)}")
        return False
    else:
        print(f"✓ All {len(methods_to_check)} methods are available")

    # Test inheritance chain
    from pygnss_rt.frontend.db_schema import QualityDatabaseSchema
    from pygnss_rt.frontend.db_insert import QualityDatabaseInsert
    from pygnss_rt.frontend.db_query import QualityDatabaseQuery
    from pygnss_rt.frontend.db_analytics import QualityDatabaseAnalytics

    if not isinstance(db, QualityDatabaseAnalytics):
        print("✗ QualityDatabase is not an instance of QualityDatabaseAnalytics")
        return False
    if not isinstance(db, QualityDatabaseQuery):
        print("✗ QualityDatabase is not an instance of QualityDatabaseQuery")
        return False
    if not isinstance(db, QualityDatabaseInsert):
        print("✗ QualityDatabase is not an instance of QualityDatabaseInsert")
        return False
    if not isinstance(db, QualityDatabaseSchema):
        print("✗ QualityDatabase is not an instance of QualityDatabaseSchema")
        return False

    print("✓ Inheritance chain verified: QualityDatabaseSchema -> QualityDatabaseInsert -> QualityDatabaseQuery -> QualityDatabaseAnalytics -> QualityDatabase")

    # Clean up
    db.close()

    return True


if __name__ == "__main__":
    print("=" * 80)
    print("Database Refactoring Verification Test")
    print("=" * 80)
    print()

    success = test_imports()

    print()
    print("=" * 80)
    if success:
        print("SUCCESS: All tests passed!")
        print("=" * 80)
        sys.exit(0)
    else:
        print("FAILURE: Some tests failed!")
        print("=" * 80)
        sys.exit(1)
