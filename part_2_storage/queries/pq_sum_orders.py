#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client pq_sum_orders.py <file_path>
'''


# Import command line arguments and helper functions
import sys
import bench

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def pq_sum_orders(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will compute the total orders grouped by zipcode

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the Parquet-backed file, e.g.,
        `hdfs:/user/{YOUR NETID}/peopleSmall.parquet

    Returns
    df_sum_orders:
        Uncomputed dataframe of total orders grouped by zipcode
    '''
    spark = SparkSession.builder.appName('pq_sum_orders').getOrCreate()
    
    # Read the CSV file into a DataFrame
    people = spark.read.csv(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    parquet_path = 'hdfs:/user/hl5679_nyu_edu/peopleSmall.parquet'
    people.write.parquet(parquet_path, mode="overwrite")
    
    df = spark.read.parquet(parquet_path)
    df.createOrReplaceTempView("people")

    total_orders = spark.sql(
        '''
        SELECT zipcode, sum(orders)
        FROM people
        GROUP by zipcode
        '''
    )
    return total_orders


def main(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    times = bench.benchmark(spark, 25, pq_sum_orders, file_path)

    print(f'Times to run \'pq_sum_orders\' Query 25 times on {file_path}')
    print(times)
    print(f'Maximum Time taken to run \'pq_sum_orders\' Query 25 times on {file_path}:{max(times)}')
    print(f'minimum Time taken to run \'pq_sum_orders\' Query 25 times on {file_path}:{min(times)}')

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    main(spark, file_path)
