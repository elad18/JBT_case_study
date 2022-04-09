"""Module to test spark app.
NOTE-assert_pyspark_df_equal will return None where there are no differences.
"""
from datetime import date

import bookings
from pyspark.sql.types import (DateType, IntegerType, LongType, StringType,
                               StructField, StructType)
from pyspark_test import assert_pyspark_df_equal

import tests.example_dataframes as test_ed

EXPECTED_SCHEMA = StructType([
    StructField("booking_ID", LongType(), True),  # moar bits for this one
    StructField("traveller_ID", IntegerType(), True),
    StructField("company_ID", IntegerType(), True),
    StructField("booking_date", DateType(), True),
    StructField("departure_date", DateType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("price_usd", IntegerType(), True),
    StructField("status", StringType(), True)
    ])


def test_date_compare_false() -> None:
    """Test that date comparison returns True when booking date > dep date."""
    booking_date = date(2021, 2, 1)
    departure_date = date(2021, 1, 1)
    assert bookings.compare_dates(booking_date, departure_date) is True


def test_date_compare_true() -> None:
    """Test that date comparison returns False when booking date < dep date."""
    booking_date = date(2021, 1, 1)
    departure_date = date(2021, 2, 1)
    assert bookings.compare_dates(booking_date, departure_date) is False


def test_pre_processing() -> None:
    """Test that pre-processing filters out where booking date > departure date."""
    test_df = bookings.pre_process(test_ed.filter_df_test)
    assert assert_pyspark_df_equal(test_df,
                                   test_ed.expected_filter_df) is None


def test_get_trips() -> None:
    """Test that get_trips properly transforms a booking dataframe to a trips one."""
    test_df = bookings.get_trips(test_ed.trip_df_test)
    test_df.show(3)
    assert assert_pyspark_df_equal(test_df,
                                   test_ed.expected_trip_df) is None


def test_get_routes() -> None:
    """Test that get_routes properly transforms a booking dataframe to one made of routes."""
    test_df = bookings.get_routes(test_ed.route_df_test)
    test_df.show(3)
    assert assert_pyspark_df_equal(test_df,
                                   test_ed.expected_route_df) is None


def test_get_travels() -> None:
    """Test that travels are filtered based on how much exchange accounts for cost."""
    test_df = bookings.get_travelers(test_ed.traveller_df_test)
    test_df.show(3)
    assert assert_pyspark_df_equal(test_df,
                                   test_ed.expected_traveller_df) is None
