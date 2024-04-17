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

    df_small = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleSmall.csv', header=True, inferSchema=True)
    df_moderate = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleModerate.csv', header=True, inferSchema=True)
    df_big = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleBig.csv', header=True, inferSchema=True)

    # Convert to Parquet and save to HDFS directory
    df_small.write.mode('overwrite').parquet('hdfs:/user/hl5679_nyu_edu/peopleSmallOpt1.parquet', mode='overwrite')
    df_moderate.write.mode('overwrite').parquet('hdfs:/user/hl5679_nyu_edu/peopleModerateOpt1.parquet', mode='overwrite')
    df_big.write.mode('overwrite').parquet('hdfs:/user/hl5679_nyu_edu/peopleBigOpt1.parquet', mode='overwrite')



# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    # spark = SparkSession.builder.appName('part2').getOrCreate()

    # Increase the replication factor to 6
    spark = SparkSession.builder.appName("part2").getOrCreate()


    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    main(spark)
    
