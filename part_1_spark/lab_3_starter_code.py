#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py
'''
import os

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType 
# from pyspark.sql.functions import expr
from pyspark.sql.functions import countDistinct


def main(spark, userID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    userID : string, userID of student to find files in HDFS
    '''
    print('Lab 3 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'hdfs:/user/{userID}/boats.txt')
    sailors = spark.read.json(f'hdfs:/user/{userID}/sailors.json')

    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    # Why does sailors already have a specified schema? -> JSON

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Give the dataframe a temporary view so we can run SQL queries
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    # Construct a query
    print('Example 1: Executing SELECT count(*) FROM boats with SparkSQL')
    query = spark.sql('SELECT count(*) FROM boats')

    # Print the results to the console
    query.show()

    #####--------------YOUR CODE STARTS HERE--------------#####

    #make sure to load reserves.json, artist_term.csv, and tracks.csv
    #For the CSVs, make sure to specify a schema!

    #question_1_query = ....

    #1.5

    #q1: sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age) in SQL 
    # select sailors older than 40
    res1 = spark.sql('SELECT sid, sname, age FROM sailors WHERE age > 40')
    res1.show()

    #q2: spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid') in object interface
    # load data and create temp view
    reserves = spark.read.json(f'hdfs:/user/{userID}/reserves.json')
    reserves.createOrReplaceTempView('reserves')

    res2 = reserves.filter(reserves.bid != 101).groupBy("sid").count()
    res2.show()

    #q3: how many distinct boats did each sailor reserve w/ a single SQL query
    #print df including the sailor's id, name, and the count of distinct boats
    res3 = spark.sql('SELECT s.sid, s.sname, COUNT(DISTINCT r.bid) AS distinct_boats_count FROM sailors s JOIN reserves r ON s.sid = r.sid GROUP BY s.sid, s.sname')   
    res3.show()


    #1.6

    #load datasets, define schema, and create temp views
    artist_term = spark.read.csv(f'hdfs:/user/{userID}/artist_term.csv', schema='artistID STRING, term STRING')
    tracks = spark.read.csv(f'hdfs:/user/{userID}/tracks.csv', schema= 'trackID STRING, title STRING, release STRING, year INT, duration FLOAT, artistID STRING')

    artist_term.createOrReplaceTempView("artist_term")
    tracks.createOrReplaceTempView("tracks")

    #q4
    #for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID). 
    #What are the results for the ten terms with the shortest average track durations?
    res4 = spark.sql("""
      SELECT 
          at.term,
          PERCENTILE_APPROX(t.year, 0.5) AS median_year_of_release,
          MAX(t.duration) AS maximum_track_duration,
          AVG(t.duration) AS average_track_duration,
          COUNT(DISTINCT at.artistID) AS total_artists
      FROM 
          artist_term at
      JOIN 
          tracks t ON at.artistID = t.artistID
      GROUP BY 
          at.term
      ORDER BY 
          AVG(t.duration) ASC
      LIMIT 10
                     """)
    res4.show()

    #q5
    #find the number of distinct tracks associated (through artistID) to each term. 
    #return only the top 10 most popular terms, and again for the bottom 10. 
    most_popular = artist_term.join(tracks, "artistID") \
        .groupBy("term") \
        .agg(countDistinct("trackID").alias("distinct_tracks_count")) \
        .orderBy("distinct_tracks_count", ascending=False) \
        .limit(10)

    least_popular = artist_term.join(tracks, "artistID") \
        .groupBy("term") \
        .agg(countDistinct("trackID").alias("distinct_tracks_count")) \
        .orderBy("distinct_tracks_count", ascending=True) \
        .limit(10)

    most_popular.show()
    least_popular.show()


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user userID from the command line
    # We need this to access the user's folder in HDFS
    userID = os.environ['USER']

    # Call our main routine
    main(spark, userID)
