#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client csv_sum_orders.py <file_path>
'''


# Import command line arguments and helper functions
import sys
import bench

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession

import numpy as np

def csv_sum_orders(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will compute the total orders grouped by zipcode

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the CSV file, e.g.,
        `hdfs:/user/pw44_nyu_edu/peopleSmall.csv`

    Returns
    df_sum_orders:
        Uncomputed dataframe of total orders grouped by zipcode
    '''

     # This loads the CSV file with proper header decoding and schema
    people = spark.read.csv(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')

    people.createOrReplaceTempView('people')

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
    for file_path in datasets:
        times = bench.benchmark(spark, 25, csv_sum_orders, file_path)

        print(f'Times to run \'csv_sum_orders\' Query 25 times on {file_path}')
        # print(times)
        print(f'Maximum Time :{max(times)}')
        print(f'minimum Time :{min(times)}')
        print(f'median Time :{np.median(times)}')
    

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    # file_path = sys.argv[1]

    datasets = [
        'hdfs:/user/pw44_nyu_edu/peopleSmall.csv',
        'hdfs:/user/pw44_nyu_edu/peopleModerate.csv',
        'hdfs:/user/pw44_nyu_edu/peopleBig.csv'
    ]

    main(spark, datasets)
