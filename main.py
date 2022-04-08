"""Task runner."""

import os

from pyspark.sql.types import (DateType, IntegerType, LongType, StringType,
                               StructField, StructType)

import bookings
from spark_handler import SparkHandler


if __name__ == '__main__':
    save_path = os.path.join(os.getcwd(), 'data/results')
    spark = SparkHandler()
    schema = StructType([
                StructField("booking_ID", LongType(), True),
                StructField("traveller_ID", IntegerType(), True),
                StructField("company_ID", IntegerType(), True),
                StructField("booking_date", DateType(), True),
                StructField("departure_date", DateType(), True),
                StructField("origin", StringType(), True),
                StructField("destination", StringType(), True),
                StructField("price_usd", IntegerType(), True),
                StructField("status", StringType(), True)
    ])
    fp = os.path.join(os.getcwd(),'data', 'bookings.csv')
    df = spark.readCSV(filepath=fp, schema=schema)
        
    # Task 1
    print("TASK 1:\n")
    filtered_df = bookings.pre_process(df)
    print(f"Filtered {df.count() - filtered_df.count()} values \n")

    filtered_df.toPandas().to_csv((os.path.join(save_path,
                                   'cleaned_bookings.csv')))

    # Task 2
    print("TASK 2:\n")
    trips_df = bookings.get_trips(filtered_df)
    trips_df.show(10)
    trips_df.toPandas().to_csv(os.path.join(save_path, 
                              'trips_results.csv'))

    # Task 3
    print("TASK 3:\n")
    rts_df = bookings.get_routes(filtered_df)
    rts_df.show(10)
    rts_df.toPandas().to_csv(os.path.join(save_path, 
                             'routes_results.csv'))

    # Task 4
    print("TASK 4:\n")
    trvl_df = bookings.get_travelers(filtered_df)
    print(f"Found {trvl_df.count()} travellers \n")
    trvl_df.toPandas().to_csv(os.path.join(save_path, 
                              'traveller_results.csv'))

    spark.stopSession()
