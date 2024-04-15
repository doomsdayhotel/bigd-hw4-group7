#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client csv_brian.py <file_path>
'''


# Import command line arguments and helper functions
import sys
import bench

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd


def csv_brian(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will filters down to only include people with `first_name`
    of 'Brian' that are not yet in the loyalty program

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the CSV file, e.g.,
        `hdfs:/user/pw44_nyu_edu/peopleSmall.csv`

    Returns
    df_brian:
        Uncomputed dataframe that only has people with 
        first_name of 'Brian' and not in the loyalty program
    '''

    people = spark.read.csv(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')

    people.createOrReplaceTempView('people')

    brian = spark.sql(
        '''
        SELECT *
        FROM people
        WHERE first_name = 'Brian' AND loyalty = 0
        '''
    )
    return brian



def main(spark, datasets):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    # for file_path in datasets:
    #     times = bench.benchmark(spark, 25, csv_brian, file_path)

    #     print(f'Times to run \'csv_brian\' Query 25 times on {file_path}')
    #     # print(times)
    #     print(f'Maximum Time :{max(times)}')
    #     print(f'minimum Time :{min(times)}')
    #     print(f'median Time :{np.median(times)}')

    timing_results = {}

    # Loop over the datasets and collect timing information
    for file_path in datasets:
        #to make sure the query successfully ran. **already checked.** 
        # df = csv_brian(spark, file_path)
        
        # print(f"Results for dataset {file_path}:")
        # df.show()


        times = bench.benchmark(spark, 25, csv_brian, file_path)

        timing_results[file_path] = {
            'min_time': min(times),
            'max_time': max(times),
            'median_time': np.median(times)
        }
        # If you want to see the results immediately after computation
        print(f'Times to run csv_brian 25 times on {file_path}:')
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

    datasets = [
        'hdfs:/user/pw44_nyu_edu/peopleSmall.csv',
        'hdfs:/user/pw44_nyu_edu/peopleModerate.csv',
        'hdfs:/user/pw44_nyu_edu/peopleBig.csv'
    ]

    main(spark, datasets)
