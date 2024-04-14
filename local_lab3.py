#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit local_lab3.py
'''
import os

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.window import Window


def main(spark, userID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    userID : string, userID of student to find files in HDFS
    '''
    print('Lab 3 Example dataframe loading and SQL query')
    
    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'/Users/lvxinyuan/me/1-Projects/NYU/1-Courses/24_Spring_Big Data/hw/homework-4-g7/boats.txt')
    sailors = spark.read.json(f'/Users/lvxinyuan/me/1-Projects/NYU/1-Courses/24_Spring_Big Data/hw/homework-4-g7/sailors.json')

    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    # Why does sailors already have a specified schema?

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Iris test
    # print("Iris test")
    # boats.show()
    # sailors.show()

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
    
    # Loading Data for Q1~Q3
    reserves = spark.read.json(f'/Users/lvxinyuan/me/1-Projects/NYU/1-Courses/24_Spring_Big Data/hw/homework-4-g7/reserves.json')
    print('Printing reserves inferred schema')
    reserves.printSchema()
    reserves.createOrReplaceTempView('reserves')

    # Questions
    # Q1
    print('Q1')
    print('Q1 spark')
    q1results = sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)
    q1results.show()
    print('Q1 SQL')
    question_1_query = spark.sql('SELECT sailors.sid, sailors.sname, sailors.age FROM sailors WHERE sailors.age > 40')
    question_1_query.show()

    # Q2
    print('Q2')
    print('Q2 spark')
    q2results = reserves.filter(reserves.bid != 101).groupby(reserves.sid).agg(functions.count(reserves.bid).alias('count_bid'))
    q2results.show()
    print('Q2 SQL')
    question_2_query = spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')
    question_2_query.show()

    # Q3
    print('Q3')
    print('Q3 spark')
    q3results = reserves.groupby(reserves.sid).agg(functions.count_distinct(reserves.bid))
    q3results.show()
    print('Q3 SQL')
    question_3_query = spark.sql('SELECT sid, count(distinct bid) FROM reserves GROUP BY sid')
    question_3_query.show()

    # Loading Data for Q4~Q5
    artist_term = spark.read.csv(f'/Users/lvxinyuan/me/1-Projects/NYU/1-Courses/24_Spring_Big Data/hw/homework-4-g7/artist_term.csv', schema='artistID STRING, term STRING')
    print('Printing artist_term inferred schema')
    artist_term.printSchema()
    artist_term.createOrReplaceTempView('artist_term')

    tracks = spark.read.csv(f'/Users/lvxinyuan/me/1-Projects/NYU/1-Courses/24_Spring_Big Data/hw/homework-4-g7/tracks.csv', schema='trackID STRING, title STRING, release STRING, year bigINT, duration DOUBLE, artistID STRING')
    print('Printing tracks inferred schema')
    tracks.printSchema()
    tracks.createOrReplaceTempView('tracks')

    # Q4
    print('Q4')
    print('Q4 spark')

    # Define window specifications for descending and ascending row numbers
    desc_window_spec = Window.partitionBy("artist_term.term").orderBy(functions.col("tracks.year").desc(), functions.col("tracks.trackID").desc())
    asc_window_spec = Window.partitionBy("artist_term.term").orderBy(functions.col("tracks.year").asc(), functions.col("tracks.trackID").asc())

    tracks_df = spark.table("tracks")
    artist_term_df = spark.table("artist_term")

    # Perform the join and add row numbers
    ranked_releases = tracks_df.join(artist_term_df, tracks_df.artistID == artist_term_df.artistID) \
        .filter(tracks_df.year != 0) \
        .select(
            artist_term_df.term.alias("term"),
            tracks_df.year,
            tracks_df.trackID,
            functions.row_number().over(desc_window_spec).alias("desc_row_num"),
            functions.row_number().over(asc_window_spec).alias("asc_row_num")
        )
    
    median_year_term = ranked_releases.filter(
        (functions.col("asc_row_num") == functions.col("desc_row_num")) |
        (functions.col("asc_row_num") == functions.col("desc_row_num") - 1) |
        (functions.col("asc_row_num") == functions.col("desc_row_num") + 1)
    ).groupBy("term") .agg(
        functions.round(functions.avg("year"), 0).cast("int").alias("median_year")
    ).orderBy(functions.col("median_year").desc())

    q4results = tracks_df.join(artist_term_df, tracks_df.artistID == artist_term_df.artistID) \
        .join(median_year_term, ["term"], "left") \
        .groupBy("term", "median_year") \
        .agg(
            functions.max("duration").alias("max_duration"),
            functions.countDistinct("tracks.artistID").alias("distinct_artist"),
            functions.avg("duration").alias("average_duration")
        ).orderBy("average_duration").limit(10)
    q4results.show()

    print('Q4 SQL')
    # question_4_query = spark.sql(
    #     '''
    #     WITH RankedReleases AS (
    #         SELECT
    #             t.term,
    #             tr.year,
    #             ROW_NUMBER() OVER (PARTITION BY t.term ORDER BY tr.year DESC, tr.trackID DESC) AS desc_row_num,
    #             ROW_NUMBER() OVER (PARTITION BY t.term ORDER BY tr.year ASC, tr.trackID ASC) AS asc_row_num
    #         FROM tracks AS tr
    #         JOIN artist_term AS t ON tr.artistID = t.artistID
    #         WHERE tr.year != 0
    #     )
    #     , MedianYearTerm AS (
    #         SELECT
    #             term,
    #             CAST(ROUND(AVG(year),0) AS INT) as median_year
    #         FROM RankedReleases
    #         WHERE
    #             asc_row_num = desc_row_num OR
    #             asc_row_num = desc_row_num - 1 OR 
    #             asc_row_num = desc_row_num + 1 
    #         GROUP BY term
    #         ORDER BY median_year DESC
    #     )
    #     SELECT 
    #         t.term
    #         , myt.median_year
    #         , max(tr.duration)
    #         , count(distinct tr.artistID) AS distinct_artist
    #         , avg(tr.duration) AS average_duration 
    #     FROM tracks AS tr
    #     JOIN artist_term AS t ON tr.artistID = t.artistID
    #     LEFT JOIN MedianYearTerm AS myt ON myt.term = t.term 
    #     GROUP BY t.term,  myt.median_year
    #     ORDER BY average_duration ASC
    #     LIMIT 10
    #     ''')
    # question_4_query.show()
    

    
    # Q5
    print('Q5')
    print('Q5 spark')
    q5results = tracks_df.join(artist_term_df, tracks_df.artistID == artist_term_df.artistID) \
    .groupBy(artist_term_df.term) \
    .agg(functions.countDistinct(tracks_df.trackID).alias("unique_track_cnt")) \
    .orderBy(functions.col("unique_track_cnt"), ascending=False) \
    .limit(10)
    q5results.show()

    print('Q5 SQL')
    question_5_query = spark.sql(
        '''
            SELECT
                t.term,
                count(distinct tr.trackID) as unique_track_cnt
            FROM tracks AS tr
            JOIN artist_term AS t ON tr.artistID = t.artistID
            GROUP BY t.term
            ORDER BY unique_track_cnt ASC
            LIMIT 10
            
        '''
    )
    question_5_query.show()

    


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user userID from the command line
    # We need this to access the user's folder in HDFS
    userID = os.environ['USER']

    # Call our main routine
    main(spark, userID)
