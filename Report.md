# Lab 3: Spark and Parquet Optimization Report

Name:

NetID:

## Part 1: Spark

#### Question 1:

How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)`?

Code:

```SQL
SELECT sid, sname, age FROM sailors WHERE age > 40
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
reserves.filter(reserves.bid != 101).groupBy("sid").count()
```

Output:

```
+---+-----+
|sid|count|
+---+-----+
| 22|    3|
| 31|    3|
| 74|    1|
| 64|    1|
+---+-----+
```

#### Question 3:

Using a single SQL query, how many distinct boats did each sailor reserve?
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats.
(Hint: you may need to use `first(...)` aggregation function on some columns.)
Provide both your query and the resulting DataFrame in your response to this question.

Code:

```SQL
SELECT s.sid, s.sname, COUNT(DISTINCT r.bid) AS distinct_boats_count FROM sailors s JOIN reserves r ON s.sid = r.sid GROUP BY s.sid, s.sname
```

Output:

```
+---+-------+--------------------+
|sid|  sname|distinct_boats_count|
+---+-------+--------------------+
| 64|horatio|                   2|
| 22|dusting|                   4|
| 31| lubber|                   3|
| 74|horatio|                   1|
+---+-------+--------------------+
```

#### Question 4:

Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID).
What are the results for the ten terms with the shortest _average_ track durations?
Include both your query code and resulting DataFrame in your response.

Code:

```python
res4 = spark.sql("""
      SELECT
          at.term,
          PERCENTILE_APPROX(t.year, 0.5) AS median_year_of_release,
          MAX(t.duration) AS maximum_track_duration,
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
```

Output:

```
+----------------+----------------------+----------------------+----------------------+-------------+
|            term|median_year_of_release|maximum_track_duration|average_track_duration|total_artists|
+----------------+----------------------+----------------------+----------------------+-------------+
|       mope rock|                     0|              13.66159|    13.661589622497559|            1|
|      murder rap|                     0|              15.46404|     15.46403980255127|            1|
|    abstract rap|                  2000|              25.91302|     25.91301918029785|            1|
|experimental rap|                  2000|              25.91302|     25.91301918029785|            1|
|     ghetto rock|                     0|              26.46159|    26.461589813232422|            1|
|  brutal rapcore|                     0|              26.46159|    26.461589813232422|            1|
|     punk styles|                     0|              41.29914|     41.29914093017578|            1|
|     turntablist|                  1993|             145.89342|     43.32922387123108|            1|
| german hardcore|                     0|              45.08689|    45.086891174316406|            1|
|     noise grind|                  2005|              89.80853|     47.68869247436523|            2|
+----------------+----------------------+----------------------+----------------------+-------------+
```

#### Question 5:

Create a query using Spark transformations that finds the number of distinct tracks associated (through artistID) to each term.
Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response.

ten most popular:

    most_popular = artist_term.join(tracks, "artistID") \
    .groupBy("term") \
    .agg(countDistinct("trackID").alias("distinct_tracks_count")) \
    .orderBy("distinct_tracks_count", ascending=False) \
    .limit(10)

    most_popular.show()

    +----------------+---------------------+
    |            term|distinct_tracks_count|
    +----------------+---------------------+
    |            rock|                21796|
    |      electronic|                17740|
    |             pop|                17129|
    |alternative rock|                11402|
    |         hip hop|                10926|
    |            jazz|                10714|
    |   united states|                10345|
    |        pop rock|                 9236|
    |     alternative|                 9209|
    |           indie|                 8569|
    +----------------+---------------------+

ten least popular:

    least_popular = artist_term.join(tracks, "artistID") \
        .groupBy("term") \
        .agg(countDistinct("trackID").alias("distinct_tracks_count")) \
        .orderBy("distinct_tracks_count", ascending=True) \
        .limit(10)

    least_popular.show()

    +--------------------+---------------------+
    |                term|distinct_tracks_count|
    +--------------------+---------------------+
    |            aberdeen|                    1|
    |         czech metal|                    1|
    |             matador|                    1|
    |         swedish rap|                    1|
    |              perlon|                    1|
    |          punk spain|                    1|
    |           kiwi rock|                    1|
    |           slovakian|                    1|
    |         angry music|                    1|
    |broermoats feesti...|                    1|
    +--------------------+---------------------+

## Part 2: Parquet Optimization:

What to include in your report:

- Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
- How do the results in parts 2.3, 2.4, and 2.5 compare?
- What did you try in part 2.5 to improve performance for each query?
- What worked, and what didn't work?

Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/
