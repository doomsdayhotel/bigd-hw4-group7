# Lab 3: Spark and Parquet Optimization Report

Name: Bess Yang, Chloe Kwon, Iris Lu
 
NetID: , N18089736

## Part 1: Spark

#### Question 1: 
How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)`?

Code:
```SQL
SELECT sailors.sid, sailors.sname, sailors.age FROM sailors WHERE sailors.age > 40
```


Output:
```
+---+-------+----+
|sid|  sname| age|
+---+-------+----+
| 22|dusting|45.0|
| 31| lubber|55.5|
| 95|    bob|63.5|
+---+-------+----+
```


#### Question 2: 
How would you express the following using the object interface instead of SQL: `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`?

Code:
```python
q2results = reserves.filter(reserves.bid != 101).groupby(reserves.sid).agg(functions.count(reserves.bid).alias('count_bid'))

q2results.show()
```


Output:
```
+---+----------+
|sid|count(bid)|
+---+----------+
| 22|         3|
| 31|         3|
| 74|         1|
| 64|         1|
+---+----------+
```

#### Question 3: 
Using a single SQL query, how many distinct boats did each sailor reserve? 
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats. 
(Hint: you may need to use `first(...)` aggregation function on some columns.) 
Provide both your query and the resulting DataFrame in your response to this question.

Code:
```SQL
SELECT sid, count(distinct bid) FROM reserves GROUP BY sid
```
```python
q3results = reserves.groupby(reserves.sid).agg(functions.count_distinct(reserves.bid))

q3results.show()
```

Output:
```
+---+-------------------+
|sid|count(DISTINCT bid)|
+---+-------------------+
| 22|                  4|
| 31|                  3|
| 74|                  1|
| 64|                  2|
+---+-------------------+
```

#### Question 4: 
Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID).
  What are the results for the ten terms with the shortest *average* track durations?
  Include both your query code and resulting DataFrame in your response.


Code:
```python
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
```

Code:
```sql
WITH RankedReleases AS (
            SELECT
                t.term,
                tr.year,
                ROW_NUMBER() OVER (PARTITION BY t.term ORDER BY tr.year DESC, tr.trackID DESC) AS desc_row_num,
                ROW_NUMBER() OVER (PARTITION BY t.term ORDER BY tr.year ASC, tr.trackID ASC) AS asc_row_num
            FROM tracks AS tr
            JOIN artist_term AS t ON tr.artistID = t.artistID
            WHERE tr.year != 0
        )
        , MedianYearTerm AS (
            SELECT
                term,
                CAST(ROUND(AVG(year),0) AS INT) as median_year
            FROM RankedReleases
            WHERE
                asc_row_num = desc_row_num OR
                asc_row_num = desc_row_num - 1 OR 
                asc_row_num = desc_row_num + 1 
            GROUP BY term
            ORDER BY median_year DESC
        )
        SELECT 
            t.term
            , myt.median_year
            , max(tr.duration)
            , count(distinct tr.artistID) AS distinct_artist
            , avg(tr.duration) AS average_duration 
        FROM tracks AS tr
        JOIN artist_term AS t ON tr.artistID = t.artistID
        LEFT JOIN MedianYearTerm AS myt ON myt.term = t.term 
        GROUP BY t.term,  myt.median_year
        ORDER BY average_duration ASC
        LIMIT 10
```


Output:
```
+----------------+-----------+-------------+---------------+-----------------+
|            term|median_year|max(duration)|distinct_artist| average_duration|
+----------------+-----------+-------------+---------------+-----------------+
|       mope rock|       NULL|     13.66159|              1|         13.66159|
|      murder rap|       NULL|     15.46404|              1|         15.46404|
|    abstract rap|       2000|     25.91302|              1|         25.91302|
|experimental rap|       2000|     25.91302|              1|         25.91302|
|     ghetto rock|       NULL|     26.46159|              1|         26.46159|
|  brutal rapcore|       NULL|     26.46159|              1|         26.46159|
|     punk styles|       NULL|     41.29914|              1|         41.29914|
|     turntablist|       1993|    145.89342|              1|43.32922428571428|
| german hardcore|       NULL|     45.08689|              1|         45.08689|
|     noise grind|       2008|     89.80853|              2|        47.688692|
+----------------+-----------+-------------+---------------+-----------------+
```
#### Question 5: 
Create a query using Spark transformations that finds the number of distinct tracks associated (through artistID) to each term.
  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
  Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response. 

Code:
```python
q5results = tracks_df.join(artist_term_df, tracks_df.artistID == artist_term_df.artistID) \
    .groupBy(artist_term_df.term) \
    .agg(functions.countDistinct(tracks_df.trackID).alias("unique_track_cnt")) \
    .orderBy(functions.col("unique_track_cnt"), ascending=False) \
    .limit(10)
```
```sql
  SELECT
      t.term,
      count(distinct tr.trackID) as unique_track_cnt
  FROM tracks AS tr
  JOIN artist_term AS t ON tr.artistID = t.artistID
  GROUP BY t.term
  ORDER BY unique_track_cnt ASC
  LIMIT 10
```

Output:
```Top 10 most popular term
+----------------+----------------+
|            term|unique_track_cnt|
+----------------+----------------+
|            rock|           21796|
|      electronic|           17740|
|             pop|           17129|
|alternative rock|           11402|
|         hip hop|           10926|
|            jazz|           10714|
|   united states|           10345|
|        pop rock|            9236|
|     alternative|            9209|
|           indie|            8569|
+----------------+----------------+
```

```Top 10 least popular term
+---------------+----------------+
|           term|unique_track_cnt|
+---------------+----------------+
|classic uk soul|               1|
|  swedish psych|               1|
|    swedish rap|               1|
|indie argentina|               1|
|      kayokyoku|               1|
|toronto hip hop|               1|
|           zulu|               1|
|         moscow|               1|
|  hand drumming|               1|
|   coupe decale|               1|
+---------------+----------------+
```


## Part 2: Parquet Optimization:

What to include in your report:
  - Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
  - How do the results in parts 2.3, 2.4, and 2.5 compare?
  - What did you try in part 2.5 to improve performance for each query?
  - What worked, and what didn't work?

Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/
