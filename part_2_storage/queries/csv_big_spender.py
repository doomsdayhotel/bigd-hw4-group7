#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client csv_big_spender.py <file_path>
'''


# Import command line arguments and helper functions
import sys
import bench

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def csv_big_spender(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will contains users with at least 100 orders but
    do not yet have a rewards card.

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the CSV file, e.g.,
        `hdfs:/user/pw44_nyu_edu/peopleSmall.csv`

    Returns
    df_big_spender:
        Uncomputed dataframe of users 
    '''

    people = spark.read.csv(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    
    people.createOrReplaceTempView('people')

    result = spark.sql('SELECT * FROM people WHERE orders >= 100 AND rewards = FALSE')
    return result


def main(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    times = bench.benchmark(spark, 25, csv_big_spender, file_path)

    min_time = min(times)
    max_time = max(times)
    median_time = np.median(times)

    print(f'Times to run Basic Query 25 times on {file_path}:')
    print(times)
    print(f'Minimum Time: {min_time}')
    print(f'Maximum Time: {max_time}')
    print(f'Median Time: {median_time}')
    
    df = csv_big_spender(spark, file_path)
    df.show()

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    main(spark, file_path)
