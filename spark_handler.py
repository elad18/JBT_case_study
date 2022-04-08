"""Module containing class to handle spark sessions."""

from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType


class SparkHandler():
    """Handler for spark sessions.

    :param spark: A spark session
    :type spark: SparkSession
    """
    
    spark = SparkSession.builder.master("local").\
        appName("cwt_test").getOrCreate()

    # Not ideal but I want to make sure answers are legible.
    spark.sparkContext.setLogLevel("ERROR")

    def readCSV(self, filepath: str, schema: StructType) -> dataframe.DataFrame:
        """Read a local .csv file into a spark DataFrame.

        :param filepath: local file path of a .csv file
        :returns: spark dataframe
        """
        return self.spark.read.csv(filepath, header=False, schema=schema)
    
    def stopSession(self):
        self.spark.stop()
