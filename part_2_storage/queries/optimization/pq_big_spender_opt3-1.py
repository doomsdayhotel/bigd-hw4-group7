#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client csv_big_spender.py <file_path>
'''


# Import command line arguments and helper functions
import sys
import bench
import numpy as np
import pandas as pd

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def pq_big_spender(spark, file_path):
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

    people = spark.read.parquet(file_path)
    
    people.createOrReplaceTempView('people')

    result = spark.sql('SELECT * FROM people WHERE orders >= 100 AND rewards = FALSE')
    return result


def main(spark, datasets):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    # times = bench.benchmark(spark, 25, csv_big_spender, file_path)

    # min_time = min(times)
    # max_time = max(times)
    # median_time = np.median(times)

    # print(f'Times to run csv_big_spender 25 times on {file_path}:')
    # print(times)
    # print(f'Minimum Time: {min_time}')
    # print(f'Maximum Time: {max_time}')
    # print(f'Median Time: {median_time}')
    
    # #to make sure the query ran successfully
    # df = csv_big_spender(spark, file_path)
    # df.show()

    timing_results = {}

    # Loop over the datasets and collect timing information
    for file_path in datasets:
        # to make sure the query successfully ran. **already checked.** 
        # df = pq_big_spender(spark, file_path)
        
        # print(f"Results for dataset {file_path}:")
        # df.show()


        times = bench.benchmark(spark, 25, pq_big_spender, file_path)

        timing_results[file_path] = {
            'min_time': min(times),
            'max_time': max(times),
            'median_time': np.median(times)
        }
        # If you want to see the results immediately after computation
        print(f'Times to run pq_big_spender 25 times on {file_path}:')
        print(timing_results[file_path])

    pd_df = pd.DataFrame(timing_results)

    pd_df.reset_index(inplace=True)
    pd_df.rename(columns={'index': 'Dataset'}, inplace=True)       

    spark_df = spark.createDataFrame(pd_df)
    spark_df.show() 

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    # file_path = sys.argv[1]

    # main(spark, file_path)

    # List of datasets to process
    datasets = [
        'hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-1.parquet',
        'hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-1.parquet',
        'hdfs:/user/qy561_nyu_edu/peopleBigOpt3-1.parquet'
    ]
    
    # Call main function with the list of datasets
    main(spark, datasets)
