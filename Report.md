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

```python
    most_popular = artist_term.join(tracks, "artistID") \
    .groupBy("term") \
    .agg(countDistinct("trackID").alias("distinct_tracks_count")) \
    .orderBy("distinct_tracks_count", ascending=False) \
    .limit(10)

    most_popular.show()
```

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

```python
    least_popular = artist_term.join(tracks, "artistID") \
        .groupBy("term") \
        .agg(countDistinct("trackID").alias("distinct_tracks_count")) \
        .orderBy("distinct_tracks_count", ascending=True) \
        .limit(10)

    least_popular.show()
```

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

  2.3:

        1. For csv_sum_orders:

            Times to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv:
            {'min_time': 0.2010793685913086, 'max_time': 5.233441114425659, 'median_time': 0.2547576427459717}

            Times to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv:
            {'min_time': 0.3351597785949707, 'max_time': 3.987332344055176, 'median_time': 0.38936924934387207}

            Times to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv:
            {'min_time': 14.800785303115845, 'max_time': 15.661024570465088, 'median_time': 14.9531090259552}

            +-----------+---------------------------------------+------------------------------------------+-------------------------------------+
            | Dataset|hdfs:/user/pw44_nyu_edu/peopleSmall.csv|hdfs:/user/pw44_nyu_edu/peopleModerate.csv|hdfs:/user/pw44_nyu_edu/peopleBig.csv|
            +-----------+---------------------------------------+------------------------------------------+-------------------------------------+
            | min_time  | 0.2010793685913086                    | 0.3351597785949707                       | 14.800785303115845                  |
            | max_time  | 5.233441114425659                     | 3.987332344055176                        | 15.661024570465088                  |
            |median_time| 0.2547576427459717                    | 0.38936924934387207                      | 14.9531090259552                    |
            +-----------+---------------------------------------+------------------------------------------+-------------------------------------+

          2. for csv_big_spender:

            Times to run csv_big_spender 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv:
            {'min_time': 0.1255478858947754, 'max_time': 4.697279453277588, 'median_time': 0.16434073448181152}

            Times to run csv_big_spender 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv:
            {'min_time': 0.21167874336242676, 'max_time': 3.172349691390991, 'median_time': 0.254932165145874}

            Times to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv:
            {'min_time': 10.536566972732544, 'max_time': 11.00087833404541, 'median_time': 10.774402618408203}

            +-----------+---------------------------------------+------------------------------------------+-------------------------------------+
            | Dataset|hdfs:/user/pw44_nyu_edu/peopleSmall.csv|hdfs:/user/pw44_nyu_edu/peopleModerate.csv|hdfs:/user/pw44_nyu_edu/peopleBig.csv|
            +-----------+---------------------------------------+------------------------------------------+-------------------------------------+
            | min_time  | 0.1255478858947754                    | 0.21167874336242676                      | 10.536566972732544                  |
            | max_time  | 4.697279453277588                     | 3.172349691390991                        | 11.00087833404541                   |
            |median_time| 0.16434073448181152                   | 0.254932165145874                        | 10.774402618408203                  |
            +-----------+---------------------------------------+------------------------------------------+-------------------------------------+

          3. for csv_brian:

            Times to run csv_brian 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv:
            {'min_time': 0.12918543815612793, 'max_time': 4.6246888637542725, 'median_time': 0.15778398513793945}

            Times to run csv_brian 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv:
            {'min_time': 0.2991602420806885, 'max_time': 3.731170892715454, 'median_time': 0.3529810905456543}

            Times to run csv_brian 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv:
            {'min_time': 11.05838680267334, 'max_time': 11.341872453689575, 'median_time': 11.13907504081726}

            +-----------+---------------------------------------+------------------------------------------+-------------------------------------+
            | Dataset|hdfs:/user/pw44_nyu_edu/peopleSmall.csv|hdfs:/user/pw44_nyu_edu/peopleModerate.csv|hdfs:/user/pw44_nyu_edu/peopleBig.csv|
            +-----------+---------------------------------------+------------------------------------------+-------------------------------------+
            | min_time  | 0.12918543815612793                   | 0.2991602420806885                       | 11.05838680267334                   |
            | max_time  | 4.6246888637542725                    | 3.731170892715454                        | 11.341872453689575                  |
            |median_time| 0.15778398513793945                   | 0.3529810905456543                       | 11.13907504081726                   |
            +-----------+---------------------------------------+------------------------------------------+-------------------------------------+

  2.4:

  \*\* I used the template to convert the csv files to parquet and saved to my hdfs direcotry, named the script `convert_csv_to_parquet.py`.

        1. For pq_sum_orders:

            Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleSmall.parquet:
            {'min_time': 0.19920039176940918, 'max_time': 3.4373297691345215, 'median_time': 0.23922491073608398}

            Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleModerate.parquet:
            {'min_time': 0.3385908603668213, 'max_time': 0.7846357822418213, 'median_time': 0.41255784034729004}

            Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleBig.parquet:
            {'min_time': 2.1765975952148438, 'max_time': 5.484586954116821, 'median_time': 2.4821882247924805}

            +-----------+--------------------------------------------+-----------------------------------------------+------------------------------------------+
            |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmall.parquet|hdfs:/user/qy561_nyu_edu/peopleModerate.parquet|hdfs:/user/qy561_nyu_edu/peopleBig.parquet|
            +-----------+--------------------------------------------+-----------------------------------------------+------------------------------------------+
            |   min_time|                         0.19920039176940918|                             0.3385908603668213|                        2.1765975952148438|
            |   max_time|                          3.4373297691345215|                             0.7846357822418213|                         5.484586954116821|
            |median_time|                         0.23922491073608398|                            0.41255784034729004|                        2.4821882247924805|
            +-----------+--------------------------------------------+-----------------------------------------------+------------------------------------------+

        2. For pq_big_spender:

            Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleSmall.parquet:
            {'min_time': 0.12046456336975098, 'max_time': 4.524384498596191, 'median_time': 0.14907360076904297}

            Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleModerate.parquet:
            {'min_time': 0.12091898918151855, 'max_time': 0.23901724815368652, 'median_time': 0.1421031951904297}

            Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleBig.parquet:
            {'min_time': 0.2972445487976074, 'max_time': 2.4234678745269775, 'median_time': 0.37487244606018066}

            +-----------+--------------------------------------------+-----------------------------------------------+------------------------------------------+
            |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmall.parquet|hdfs:/user/qy561_nyu_edu/peopleModerate.parquet|hdfs:/user/qy561_nyu_edu/peopleBig.parquet|
            +-----------+--------------------------------------------+-----------------------------------------------+------------------------------------------+
            |   min_time|                         0.12046456336975098|                            0.12091898918151855|                        0.2972445487976074|
            |   max_time|                           4.524384498596191|                            0.23901724815368652|                        2.4234678745269775|
            |median_time|                         0.14907360076904297|                             0.1421031951904297|                       0.37487244606018066|
            +-----------+--------------------------------------------+-----------------------------------------------+------------------------------------------+

        3. For pq_brian:

            Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleSmall.parquet:
            {'min_time': 0.1450357437133789, 'max_time': 2.3414812088012695, 'median_time': 0.2513551712036133}

            Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleModerate.parquet:
            {'min_time': 0.1476607322692871, 'max_time': 0.47472572326660156, 'median_time': 0.2550804615020752}

            Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleBig.parquet:
            {'min_time': 3.657675266265869, 'max_time': 4.881026744842529, 'median_time': 4.08001971244812}

            +-----------+--------------------------------------------+-----------------------------------------------+------------------------------------------+
            |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmall.parquet|hdfs:/user/qy561_nyu_edu/peopleModerate.parquet|hdfs:/user/qy561_nyu_edu/peopleBig.parquet|
            +-----------+--------------------------------------------+-----------------------------------------------+------------------------------------------+
            |   min_time|                          0.1450357437133789|                             0.1476607322692871|                         3.657675266265869|
            |   max_time|                          2.3414812088012695|                            0.47472572326660156|                         4.881026744842529|
            |median_time|                          0.2513551712036133|                             0.2550804615020752|                          4.08001971244812|
            +-----------+--------------------------------------------+-----------------------------------------------+------------------------------------------+

- How do the results in parts 2.3, 2.4, and 2.5 compare?

- What did you try in part 2.5 to improve performance for each query?
- What worked, and what didn't work?

Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/
