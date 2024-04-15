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
    # Read the CSV files
    # or instead of inferSchema, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN'? I forgot which one I used, but it worked
    
    df_small = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleSmall.csv', header=True, inferSchema=True)
    df_moderate = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleModerate.csv', header=True, inferSchema=True)
    df_big = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleBig.csv', header=True, inferSchema=True)

    df_small_sorted = df_small.sort(col("orders"), col("zipcode"))
    df_moderate_sorted = df_moderate.sort(col("orders"), col("zipcode"))
    df_big_sorted = df_big.sort(col("orders"), col("zipcode"))


    # Convert to Parquet and save to HDFS directory
    df_small_sorted.write.parquet('hdfs:/user/qy561_nyu_edu/peopleSmallOpt1SumOrders.parquet')
    df_moderate_sorted.write.parquet('hdfs:/user/qy561_nyu_edu/peopleModerateOpt1SumOrders.parquet')
    df_big_sorted.write.parquet('hdfs:/user/qy561_nyu_edu/peopleBigOpt1SumOrders.parquet')

    # use the following code to check and preview parquet files
    # Read Parquet file into DataFrame
    # df = spark.read.parquet('hdfs:/user/qy561_nyu_edu/peopleModerate.parquet')  h

    # # Show contents of DataFrame
    # df.show()

    # # Print schema of DataFrame
    # df.printSchema()

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    main(spark)
    
