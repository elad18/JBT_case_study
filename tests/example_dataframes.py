"""Module containing spark dataframes for validation testing."""

from datetime import date

from pyspark.sql import Row, SparkSession

spark = SparkSession.builder.getOrCreate()

# Q1 scenarios - 
# Filter those where booking date > dep. date
filter_df_test = spark.createDataFrame([
    Row(booking_id=1, traveller_ID=4, company_ID='GFG1',
        booking_date=date(2022, 1, 1), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Japan', price_usd=128,
        status='BOOKING'),

    Row(booking_id=1, traveller_ID=4, company_ID='GFG1',
        booking_date=date(2020, 9, 1), departure_date=date(2020, 10, 1),
        origin='Albania', destination='Kenya', price_usd=128,
        status='BOOKING'),

    Row(booking_id=1, traveller_ID=4, company_ID='GFG1',
        booking_date=date(2022, 9, 1), departure_date=date(2022, 8, 1),
        origin='GME', destination='the moon', price_usd=128,
        status='BOOKING'),

    Row(booking_id=1, traveller_ID=4, company_ID='GFG1',
        booking_date=date(2021, 11, 1), departure_date=date(2021, 11, 21),
        origin='Cambodia', destination='Spain', price_usd=128,
        status='BOOKING'),
])

expected_filter_df = spark.createDataFrame([
    Row(booking_id=1, traveller_ID=4, company_ID='GFG1',
        booking_date=date(2020, 9, 1), departure_date=date(2020, 10, 1),
        origin='Albania', destination='Kenya', price_usd=128,
        status='BOOKING'),

    Row(booking_id=1, traveller_ID=4, company_ID='GFG1',
        booking_date=date(2021, 11, 1), departure_date=date(2021, 11, 21),
        origin='Cambodia', destination='Spain', price_usd=128,
        status='BOOKING'),
])

# Q2 scenarios -
# Test cases for condensing a booking data frame to a trip data frame.
# We are looking for:
# 1) Correct price aggregation
# 2) Consistent mappings of booking identifiers from booking_df -> trip_df
# 3) Resultant trip dataframe is sorted in descending order by agg. price

trip_df_test = spark.createDataFrame([
    Row(booking_id=1, traveller_ID=1, company_ID='1',
        booking_date=date(2020, 1, 1), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Japan', price_usd=128,
        status='BOOKING'),

    Row(booking_id=1, traveller_ID=1, company_ID='1',
        booking_date=date(2020, 1, 1), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Japan', price_usd=1,
        status='EXCHANGED'),

    Row(booking_id=1, traveller_ID=1, company_ID='1',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Japan', price_usd=1,
        status='EXCHANGED'),

    Row(booking_id=2, traveller_ID=2, company_ID='2',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Albania', destination='Kenya', price_usd=1,
        status='BOOKING'),

    Row(booking_id=3, traveller_ID=3, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=1,
        status='BOOKING'),

    Row(booking_id=3, traveller_ID=3, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=1,
        status='EXCHANGED'),

    Row(booking_id=3, traveller_ID=3, company_ID='3',
        booking_date=date(2020, 6, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=1,
        status='CANCELLATION'),
])

expected_trip_df = spark.createDataFrame([
    Row(trip_ID=1, traveller_ID=1, company_ID='1',
        trip_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Japan', price_usd=130),

    Row(trip_ID=3, traveller_ID=3, company_ID='3',
        trip_date=date(2020, 6, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=3),

    Row(trip_ID=2, traveller_ID=2, company_ID='2',
        trip_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Albania', destination='Kenya', price_usd=1),
])

# Q3 scenarios - 
# Test cases for get_route function. We are looking for:
# 1) Is only the most expensive route for a company returned?
# 2) Are price aggregations correct?
# 3) Are row entries for the route field constructed with proper formatting?

route_df_test = spark.createDataFrame([

    # Row index 1 should be selected (100 < 128 < 300)

    Row(booking_id=1, traveller_ID=1, company_ID='1',
        booking_date=date(2020, 1, 1), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Japan', price_usd=128,
        status='BOOKING'),

    Row(booking_id=2, traveller_ID=1, company_ID='1',
        booking_date=date(2020, 1, 1), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Japan', price_usd=300,
        status='EXCHANGED'),

    Row(booking_id=3, traveller_ID=1, company_ID='1',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Portugal', price_usd=100,
        status='EXCHANGED'),

    # Only entry for company ID 2, so should still show up.

    Row(booking_id=4, traveller_ID=2, company_ID='2',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Albania', destination='Kenya', price_usd=1,
        status='BOOKING'),

    # Row index 5 should be selected (1 = 1 < 1000)

    Row(booking_id=5, traveller_ID=3, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=1,
        status='BOOKING'),

    Row(booking_id=6, traveller_ID=3, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='India', destination='Barbados', price_usd=1000,
        status='EXCHANGED'),

    Row(booking_id=7, traveller_ID=3, company_ID='3',
        booking_date=date(2020, 6, 2), departure_date=date(2020, 12, 12),
        origin='USA', destination='Canada', price_usd=1,
        status='CANCELLATION'),
])

expected_route_df = spark.createDataFrame([
    Row(company_ID='3', route='India-Barbados', price_usd=1000),
    Row(company_ID='1', route='Morocco-Japan', price_usd=428),
    Row(company_ID='2', route='Albania-Kenya', price_usd=1),

])

# Q4 Scenarios 
# Test cases for get_traveller function.
# We assess for:
# Do we get the two most frequently exchanged travllers for a given client?
# Do costs of exchanges account for at least 8% of total travel costs?
# Are %'s less than 8% filtered out?
# Do we still keep travellers whose % cost = 8%? 
# Do clients with no travellers whose cost >= 8% show up? (they shouldn't!)
traveller_df_test = spark.createDataFrame([

    # scenario 1 - a traveller who has exchanged more than 8% of cost,
    # and therefore remains.
    Row(booking_id=1, traveller_ID=1, company_ID='1',
        booking_date=date(2020, 1, 1), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Japan', price_usd=300,
        status='BOOKING'),

    Row(booking_id=2, traveller_ID=1, company_ID='1',
        booking_date=date(2020, 1, 1), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Japan', price_usd=128,
        status='EXCHANGED'),

    # scenario 2 - a traveller who has exchanged loads but not more than 8%,
    # and so is filtered out.
    Row(booking_id=3, traveller_ID=2, company_ID='1',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Portugal', price_usd=100,
        status='BOOKING'),

    Row(booking_id=4, traveller_ID=2, company_ID='1',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Portugal', price_usd=1,
        status='EXCHANGED'),

    Row(booking_id=5, traveller_ID=2, company_ID='1',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Morocco', destination='Portugal', price_usd=1,
        status='EXCHANGED'),

    # scenario 3 - a traveller who has not exchanged at all,
    # and so should be filtered out (meaning comp_ID=2 shouldn't show!)
    Row(booking_id=6, traveller_ID=3, company_ID='2',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Albania', destination='Kenya', price_usd=1,
        status='BOOKING'),

    # scenario 4 - a company who has N>2 travellers exchanged more than
    # 8% of cost. Does it pick out the top 2?
    # traveller_id = 3 -> 37.5%
    # traveller_id = 4 -> 45%
    # traveller_id = 5 -> 11%
    Row(booking_id=7, traveller_ID=3, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=25,
        status='BOOKING'),

    Row(booking_id=8, traveller_ID=3, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=10,
        status='EXCHANGED'),

    Row(booking_id=9, traveller_ID=3, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=5,
        status='EXCHANGED'),

    Row(booking_id=10, traveller_ID=4, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=26,
        status='BOOKING'),

    Row(booking_id=11, traveller_ID=4, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=16,
        status='EXCHANGED'),

    Row(booking_id=12, traveller_ID=4, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=5,
        status='EXCHANGED'),

    Row(booking_id=13, traveller_ID=5, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=8,
        status='BOOKING'),

    Row(booking_id=14, traveller_ID=5, company_ID='3',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=1,
        status='EXCHANGED'),

    # scenario 5 - a company who has an employee with exactly
    # 8% of cost. Should remain in the dataset.
    Row(booking_id=15, traveller_ID=6, company_ID='4',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=1000,
        status='BOOKING'),

    Row(booking_id=16, traveller_ID=6, company_ID='4',
        booking_date=date(2020, 1, 2), departure_date=date(2020, 12, 12),
        origin='Nepal', destination='Japan', price_usd=87,
        status='EXCHANGED'),
])

expected_traveller_df = spark.createDataFrame([
    Row(traveller_ID=1, company_ID='1', exchange_perc=30.0),
    Row(traveller_ID=4, company_ID='3', exchange_perc=45.0),
    Row(traveller_ID=3, company_ID='3', exchange_perc=38.0),
    Row(traveller_ID=6, company_ID='4', exchange_perc=8.0)
])
