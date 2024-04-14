#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Template script to connect to Active Spark Session
Usage:
    $ spark-submit --deploy-mode client lab_3_storage_template_code.py <any arguments you wish to add>
'''


# Import command line arguments and helper functions(if necessary)
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def main(spark):
    '''Main routine for run for Storage optimization template.
    Parameters
    ----------
    spark : SparkSession object

    '''
    #####--------------YOUR CODE STARTS HERE--------------#####

    #Use this template to as much as you want for your parquet saving and optimizations!
    # # Read the CSV files
    # df_small = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleSmall.csv', header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    # df_moderate = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleModerate.csv', header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    # df_big = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleBig.csv', header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')

    # # Convert to Parquet and save to HDFS directory
    # df_small.write.mode('overwrite').parquet('hdfs:/user/qy561_nyu_edu/peopleSmall.parquet')
    # df_moderate.write.mode('overwrite').parquet('hdfs:/user/qy561_nyu_edu/peopleModerate.parquet')
    # df_big.write.mode('overwrite').parquet('hdfs:/user/qy561_nyu_edu/peopleBig.parquet')

    # Replace the path with the actual path to your Parquet file
    parquet_file_path = 'hdfs:/user/qy561_nyu_edu/peopleSmall.parquet'

    # Read the Parquet file
    df = spark.read.parquet(parquet_file_path)

    # Show the first few rows of the DataFrame
    df.show()

    # Print the schema of the DataFrame
    df.printSchema()


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    main(spark)
    
