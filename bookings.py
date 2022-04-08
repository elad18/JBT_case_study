"""Module to transform bookings data."""

import datetime

from pyspark.sql import dataframe, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


def compare_dates(booking_date: datetime.date, departure_date:
                  datetime.date) -> bool:
    """Compare whether booking date is greater than departure date.

    :param booking_date: the date a booking was made
    :param departure_date: the departure date of a booking
    """
    return bool(booking_date > departure_date)


def pre_process(df: dataframe.DataFrame) -> dataframe.DataFrame:
    """Clean dataset of temporally invalid row instances.

    :param df: Dataset to be cleansed
    :returns: Cleaned dataset
    """
    # Using operator messes it up, why?
    filter_invalid_udf = F.udf(compare_dates, BooleanType())
    filtered = df.withColumn('bool',
                             filter_invalid_udf('booking_date',
                                                'departure_date'))\
                 .filter(~F.col('bool')).drop('bool')
    return filtered


def get_trips(df: dataframe.DataFrame) -> dataframe.DataFrame:
    """Transform a booking dataframe to get trips.

    :param df: Dataset to be transformed
    :returns: Dataset of all trips
    """
    trip_df = df.groupBy("booking_ID",
                         "traveller_ID",
                         "company_ID",
                         "departure_date",
                         "origin",
                         "destination")\
                .agg(F.sum("price_usd").alias("price_usd"),
                     F.max("booking_date").alias("trip_date"))\
                .orderBy(F.desc('price_usd'))\
                .withColumnRenamed("booking_ID", "trip_ID")

    return trip_df


def get_routes(df: dataframe.DataFrame) -> dataframe.DataFrame:
    """Transform a booking dataframe to get all routes for a company.

    :param df: DataFrame to be transformed
    :returns: DataFrame mapping companies to a route with summed prices.
    """
    route_window = Window.partitionBy("company_ID")\
                         .orderBy(F.col("price_usd").desc())

    route_df = df.select(F.concat(F.col("origin"), F.lit("-"),
                         F.col("destination")).alias("route"),
                         "company_ID",
                         "price_usd")\
                 .groupBy("route", "company_ID")\
                 .agg(F.sum("price_usd").alias("price_usd"))

    route_df = route_df.withColumn("row_num",
                                   F.row_number().over(route_window))\
                       .filter(F.col("row_num") == 1)\
                       .drop("row_num")\
                       .orderBy(F.desc('price_usd'))
    return route_df


def get_travelers(df: dataframe.DataFrame) -> dataframe.DataFrame:
    """Transform booking data to get top two travellers of a company
    where exchanges accounts for at least 8% of total travel cost.

    :param df: DataFrame to be transformed
    :returns: DataFrame mapping companies to a route with summed prices.
    """
    # We want the groupings for exchanges divided by sum across all status.
    w1 = Window.partitionBy("traveller_ID",
                            "company_ID",
                            "status")

    w2 = Window.partitionBy("traveller_ID",
                            "company_ID")

    res = df.withColumn('exchange_perc', F.sum("price_usd")
                        .over(w1)/F.sum("price_usd").over(w2))

    # We only care for exchanges where price > 8%
    res = res.select("traveller_ID", "company_ID", "status",
                     "exchange_perc").distinct()\
             .filter(F.col('status') == 'EXCHANGED').drop('status')

    res = res.withColumn("exchange_perc", F.round(F.col("exchange_perc"), 2) * 100)\
             .filter(F.col('exchange_perc') >= 8)

    # Sort by price and get the 2 highest percentages for a company.
    w3 = Window.partitionBy("company_ID")\
               .orderBy(F.col("exchange_perc").desc())

    res = res.withColumn("row", F.row_number().over(w3))\
             .filter(F.col("row") <= 2.)\
             .drop('row')
    return res
