# Lab 3: Spark and Parquet Optimization Report

Name: Bess Yang

NetID: qy561

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

\*\* I wrote a seperate script to convert csv files to parquet, named the script `csv_to_parquet_2-4.py` under the folder queries.

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

2.5: Optimization

    - Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
    - What did you try in part 2.5 to improve performance for each query?
    - How do the results in parts 2.3, 2.4, and 2.5 compare?
    - What worked, and what didn't work?

\*\* See the optimization folder under optimization folder. For each query, there are two files: one for converting from csv to parquet files after implementing optimization methods and another script for running and benchmarking queries.

        1. Optimization Method #1: Sort Columns

            A. For pq_sum_orders:

                Sorted column 'zipcode'.

                ```python
                df_small_sorted = df_small.sort(col("zipcode"))
                df_moderate_sorted = df_moderate.sort(col("zipcode"))
                df_big_sorted = df_big.sort(col("zipcode"))
                ```

                Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt1SumOrders.parquet:
                {'min_time': 0.19478535652160645, 'max_time': 5.254602670669556, 'median_time': 0.27867722511291504}

                Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt1SumOrders.parquet:
                {'min_time': 0.23452329635620117, 'max_time': 0.3518366813659668, 'median_time': 0.2500739097595215}

                Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt1SumOrders.parquet:
                {'min_time': 3.8291749954223633, 'max_time': 4.405329465866089, 'median_time': 3.90116810798645}

                +-----------+---------------------------------------------------------+------------------------------------------------------------+-------------------------------------------------------+
                |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt1SumOrders.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt1SumOrders.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt1SumOrders.parquet|
                +-----------+---------------------------------------------------------+------------------------------------------------------------+-------------------------------------------------------+
                |   min_time|                                      0.19478535652160645|                                         0.23452329635620117|                                     3.8291749954223633|
                |   max_time|                                        5.254602670669556|                                          0.3518366813659668|                                      4.405329465866089|
                |median_time|                                      0.27867722511291504|                                          0.2500739097595215|                                       3.90116810798645|
                +-----------+---------------------------------------------------------+------------------------------------------------------------+-------------------------------------------------------+

            B. for pq_big_spender:

                Sorted columns 'orders' (desc) and 'rewards'.

                ```python
                df_small_sorted = df_small.sort(col("orders").desc(), col("rewards"))
                df_moderate_sorted = df_moderate.sort(col("orders").desc(), col("rewards"))
                df_big_sorted = df_big.sort(col("orders").desc(), col("rewards"))
                ```
                Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt1BigSpender.parquet:
                {'min_time': 0.13169550895690918, 'max_time': 2.5849785804748535, 'median_time': 0.1781754493713379}

                Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt1BigSpender.parquet:
                {'min_time': 0.11814355850219727, 'max_time': 1.4290950298309326, 'median_time': 0.14305806159973145}

                Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt1BigSpender.parquet:
                {'min_time': 0.13505053520202637, 'max_time': 0.19906377792358398, 'median_time': 0.14879655838012695}

                +-----------+----------------------------------------------------------+-------------------------------------------------------------+--------------------------------------------------------+
                |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt1BigSpender.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt1BigSpender.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt1BigSpender.parquet|
                +-----------+----------------------------------------------------------+-------------------------------------------------------------+--------------------------------------------------------+
                |   min_time|                                       0.13169550895690918|                                          0.11814355850219727|                                     0.13505053520202637|
                |   max_time|                                        2.5849785804748535|                                           1.4290950298309326|                                     0.19906377792358398|
                |median_time|                                        0.1781754493713379|                                          0.14305806159973145|                                     0.14879655838012695|
                +-----------+----------------------------------------------------------+-------------------------------------------------------------+--------------------------------------------------------+

            C. for pq_brian:

                Sorted columns 'first_name' and 'loyalty'.

                ```python
                df_small_sorted = df_small.sort(col("first_name"), col("loyalty"))
                df_moderate_sorted = df_moderate.sort(col("first_name"), col("loyalty"))
                df_big_sorted = df_big.sort(col("first_name"), col("loyalty"))
                ```

                Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt1Brian.parquet:
                {'min_time': 0.13490867614746094, 'max_time': 2.527125835418701, 'median_time': 0.17870140075683594}

                Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt1Brian.parquet:
                {'min_time': 0.1221778392791748, 'max_time': 1.3962531089782715, 'median_time': 0.13815689086914062}

                Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt1Brian.parquet:
                {'min_time': 3.356139898300171, 'max_time': 3.961683750152588, 'median_time': 3.897855758666992}

                +-----------+-----------------------------------------------------+--------------------------------------------------------+---------------------------------------------------+
                |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt1Brian.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt1Brian.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt1Brian.parquet|
                +-----------+-----------------------------------------------------+--------------------------------------------------------+---------------------------------------------------+
                |   min_time|                                  0.13490867614746094|                                      0.1221778392791748|                                  3.356139898300171|
                |   max_time|                                    2.527125835418701|                                      1.3962531089782715|                                  3.961683750152588|
                |median_time|                                  0.17870140075683594|                                     0.13815689086914062|                                  3.897855758666992|
                +-----------+-----------------------------------------------------+--------------------------------------------------------+---------------------------------------------------+

        2. Optimization Method #2 Change the replication factor

            Used `hadoop fs -ls <file path>` to get default replication factor.

            default replication factor is : 1

            Setting the replication factor to 3 becuase it is the commonly adopted and a fair trade-off between fault tolerance and storage cost.

            ```python
            if __name__ == "__main__":
            spark = SparkSession.builder.appName('part2').config("spark.hadoop.dfs.replication", "3").getOrCreate()
            ...
            ```

            A. for pq_sum_orders:

                Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt2.parquet:
                {'min_time': 0.18006467819213867, 'max_time': 5.114980459213257, 'median_time': 0.24790668487548828}

                Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt2.parquet:
                {'min_time': 0.31557226181030273, 'max_time': 0.6819477081298828, 'median_time': 0.38585996627807617}

                Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt2.parquet:
                {'min_time': 3.0053720474243164, 'max_time': 3.9871532917022705, 'median_time': 3.2145068645477295}

                +-----------+------------------------------------------------+---------------------------------------------------+----------------------------------------------+
                |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt2.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt2.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt2.parquet|
                +-----------+------------------------------------------------+---------------------------------------------------+----------------------------------------------+
                |   min_time|                             0.18006467819213867|                                0.31557226181030273|                            3.0053720474243164|
                |   max_time|                               5.114980459213257|                                 0.6819477081298828|                            3.9871532917022705|
                |median_time|                             0.24790668487548828|                                0.38585996627807617|                            3.2145068645477295|
                +-----------+------------------------------------------------+---------------------------------------------------+----------------------------------------------+

            B. for pq_big_spender:

                Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt2.parquet:
                {'min_time': 0.1247096061706543, 'max_time': 5.53405499458313, 'median_time': 0.16304755210876465}

                Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt2.parquet:
                {'min_time': 0.11816835403442383, 'max_time': 0.2691013813018799, 'median_time': 0.16377043724060059}

                Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt2.parquet:
                {'min_time': 0.5906436443328857, 'max_time': 1.6331274509429932, 'median_time': 0.7588903903961182}

                +-----------+------------------------------------------------+---------------------------------------------------+----------------------------------------------+
                |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt2.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt2.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt2.parquet|
                +-----------+------------------------------------------------+---------------------------------------------------+----------------------------------------------+
                |   min_time|                              0.1247096061706543|                                0.11816835403442383|                            0.5906436443328857|
                |   max_time|                                5.53405499458313|                                 0.2691013813018799|                            1.6331274509429932|
                |median_time|                             0.16304755210876465|                                0.16377043724060059|                            0.7588903903961182|
                +-----------+------------------------------------------------+---------------------------------------------------+----------------------------------------------+

            C. for pq_brian:

                Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt2.parquet:
                {'min_time': 0.1188971996307373, 'max_time': 4.546337842941284, 'median_time': 0.14828276634216309}

                Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt2.parquet:
                {'min_time': 0.13617610931396484, 'max_time': 0.2509911060333252, 'median_time': 0.17609286308288574}

                Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt2.parquet:
                {'min_time': 1.604921579360962, 'max_time': 4.219437122344971, 'median_time': 2.4211385250091553}

                +-----------+------------------------------------------------+---------------------------------------------------+----------------------------------------------+
                |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt2.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt2.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt2.parquet|
                +-----------+------------------------------------------------+---------------------------------------------------+----------------------------------------------+
                |   min_time|                              0.1188971996307373|                                0.13617610931396484|                             1.604921579360962|
                |   max_time|                               4.546337842941284|                                 0.2509911060333252|                             4.219437122344971|
                |median_time|                             0.14828276634216309|                                0.17609286308288574|                            2.4211385250091553|
                +-----------+------------------------------------------------+---------------------------------------------------+----------------------------------------------+


        3. Optimization Method #3: Repartitioning

            3-1: Setting the number of partitions

                from `get_config`:

                Number of executors: 1
                Total number of cores: 16

                Thus, trying 36 partitioins:

                ```python
                df_small_repartitioned = df_small.repartition(36)
                df_moderate_repartitioned = df_moderate.repartition(36)
                df_big_repartitioned = df_big.repartition(36)
                ```
                A. for pq_sum_orders:

                    Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-1.parquet:
                    {'min_time': 0.22266745567321777, 'max_time': 6.111672401428223, 'median_time': 0.3198375701904297}

                    Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-1.parquet:
                    {'min_time': 3.623654365539551, 'max_time': 4.308531045913696, 'median_time': 3.9093332290649414}

                    Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt3-1.parquet:
                    {'min_time': 5.410287618637085, 'max_time': 7.296245098114014, 'median_time': 6.579694747924805}

                    +-----------+--------------------------------------------------+-----------------------------------------------------+------------------------------------------------+
                    |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-1.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-1.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt3-1.parquet|
                    +-----------+--------------------------------------------------+-----------------------------------------------------+------------------------------------------------+
                    |   min_time|                               0.22266745567321777|                                    3.623654365539551|                               5.410287618637085|
                    |   max_time|                                 6.111672401428223|                                    4.308531045913696|                               7.296245098114014|
                    |median_time|                                0.3198375701904297|                                   3.9093332290649414|                               6.579694747924805|
                    +-----------+--------------------------------------------------+-----------------------------------------------------+------------------------------------------------+

                B. for pq_big_spender:

                    Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-1.parquet:
                    {'min_time': 0.20514798164367676, 'max_time': 5.031052827835083, 'median_time': 0.267803430557251}

                    Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-1.parquet:
                    {'min_time': 3.779075860977173, 'max_time': 4.11845588684082, 'median_time': 3.893751621246338}

                    Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt3-1.parquet:
                    {'min_time': 3.6144943237304688, 'max_time': 4.329789876937866, 'median_time': 3.8564276695251465}

                    +-----------+--------------------------------------------------+-----------------------------------------------------+------------------------------------------------+
                    |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-1.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-1.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt3-1.parquet|
                    +-----------+--------------------------------------------------+-----------------------------------------------------+------------------------------------------------+
                    |   min_time|                               0.20514798164367676|                                    3.779075860977173|                              3.6144943237304688|
                    |   max_time|                                 5.031052827835083|                                     4.11845588684082|                               4.329789876937866|
                    |median_time|                                 0.267803430557251|                                    3.893751621246338|                              3.8564276695251465|
                    +-----------+--------------------------------------------------+-----------------------------------------------------+------------------------------------------------+

                C. for pq_brian:

                    Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-1.parquet:
                    {'min_time': 0.14253926277160645, 'max_time': 5.864479303359985, 'median_time': 0.18588781356811523}

                    Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-1.parquet:
                    {'min_time': 0.1454927921295166, 'max_time': 0.5632948875427246, 'median_time': 0.1642611026763916}

                    Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt3-1.parquet:
                    {'min_time': 1.0210862159729004, 'max_time': 1.965998888015747, 'median_time': 1.0468099117279053}

                    +-----------+--------------------------------------------------+-----------------------------------------------------+------------------------------------------------+
                    |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-1.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-1.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt3-1.parquet|
                    +-----------+--------------------------------------------------+-----------------------------------------------------+------------------------------------------------+
                    |   min_time|                               0.14253926277160645|                                   0.1454927921295166|                              1.0210862159729004|
                    |   max_time|                                 5.864479303359985|                                   0.5632948875427246|                               1.965998888015747|
                    |median_time|                               0.18588781356811523|                                   0.1642611026763916|                              1.0468099117279053|
                    +-----------+--------------------------------------------------+-----------------------------------------------------+------------------------------------------------+

            3.2: Partition by columns

                A. for pq_sum_orders:

                    Repartitioned by column 'zipcode'.

                    ```python
                    df_small_repartitioned = df_small.repartition(col("zipcode"))
                    df_moderate_repartitioned = df_moderate.repartition(col("zipcode"))
                    df_big_repartitioned = df_big.repartition(col("zipcode"))
                    ```

                    Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-2SumOrders.parquet:
                    {'min_time': 0.18282175064086914, 'max_time': 8.369690895080566, 'median_time': 0.2533435821533203}

                    Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-2SumOrders.parquet:
                    {'min_time': 3.556148052215576, 'max_time': 4.113949298858643, 'median_time': 3.896993637084961}

                    Times to run pq_sum_orders 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt3-2SumOrders.parquet:
                    {'min_time': 3.6304237842559814, 'max_time': 4.363723516464233, 'median_time': 3.9076151847839355}

                    +-----------+-----------------------------------------------------------+--------------------------------------------------------------+---------------------------------------------------------+
                    |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-2SumOrders.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-2SumOrders.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt3-2SumOrders.parquet|
                    +-----------+-----------------------------------------------------------+--------------------------------------------------------------+---------------------------------------------------------+
                    |   min_time|                                        0.18282175064086914|                                             3.556148052215576|                                       3.6304237842559814|
                    |   max_time|                                          8.369690895080566|                                             4.113949298858643|                                        4.363723516464233|
                    |median_time|                                         0.2533435821533203|                                             3.896993637084961|                                       3.9076151847839355|
                    +-----------+-----------------------------------------------------------+--------------------------------------------------------------+---------------------------------------------------------+

                B. for pq_big_spender:

                    Repartitioned by column 'orders'.

                    ```python
                    df_small_repartitioned = df_small.repartition(col("orders"))
                    df_moderate_repartitioned = df_moderate.repartition(col("orders"))
                    df_big_repartitioned = df_big.repartition(col("orders"))
                    ```
                    Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-2BigSpender.parquet:
                    {'min_time': 0.1383836269378662, 'max_time': 4.940512657165527, 'median_time': 0.17888879776000977}

                    Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-2BigSpender.parquet:
                    {'min_time': 0.10951995849609375, 'max_time': 1.4327480792999268, 'median_time': 0.13005518913269043}

                    Times to run pq_big_spender 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt3-2BigSpender.parquet:
                    {'min_time': 3.820300340652466, 'max_time': 4.110840559005737, 'median_time': 3.9002137184143066}

                    +-----------+------------------------------------------------------------+---------------------------------------------------------------+----------------------------------------------------------+
                    |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-2BigSpender.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-2BigSpender.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt3-2BigSpender.parquet|
                    +-----------+------------------------------------------------------------+---------------------------------------------------------------+----------------------------------------------------------+
                    |   min_time|                                          0.1383836269378662|                                            0.10951995849609375|                                         3.820300340652466|
                    |   max_time|                                           4.940512657165527|                                             1.4327480792999268|                                         4.110840559005737|
                    |median_time|                                         0.17888879776000977|                                            0.13005518913269043|                                        3.9002137184143066|
                    +-----------+------------------------------------------------------------+---------------------------------------------------------------+----------------------------------------------------------+


                C. for pq_brian:

                    Repartitioning for this query is not a good idea. The column 'Loyalty' has low cardinality and will only lead to two partitions. The column 'first_name' has high cardinality (a large number of partitions but little data), and it might have an uneven distribution of names, leading to uneven distributions of partitions.

                    However, I'll still repartition by column 'frist_name' for the sake of completing this task/for the sake of seeing what the results look like. It is expected repartitioning here will slow down processing instead of speeding it up.

                    ```python
                    df_small_repartitioned = df_small.repartition(col("first_name"))
                    df_moderate_repartitioned = df_moderate.repartition(col("first_name"))
                    df_big_repartitioned = df_big.repartition(col("first_name"))
                    ```

                    Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-2Brian.parquet:
                    {'min_time': 0.1379683017730713, 'max_time': 6.104769229888916, 'median_time': 0.17804932594299316}

                    Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-2Brian.parquet:
                    {'min_time': 0.12296414375305176, 'max_time': 0.1788628101348877, 'median_time': 0.1428384780883789}

                    Times to run pq_brian 25 times on hdfs:/user/qy561_nyu_edu/peopleBigOpt3-2Brian.parquet:
                    {'min_time': 0.2393805980682373, 'max_time': 0.3773176670074463, 'median_time': 0.26538968086242676}

                    +-----------+-------------------------------------------------------+----------------------------------------------------------+-----------------------------------------------------+
                    |    Dataset|hdfs:/user/qy561_nyu_edu/peopleSmallOpt3-2Brian.parquet|hdfs:/user/qy561_nyu_edu/peopleModerateOpt3-2Brian.parquet|hdfs:/user/qy561_nyu_edu/peopleBigOpt3-2Brian.parquet|
                    +-----------+-------------------------------------------------------+----------------------------------------------------------+-----------------------------------------------------+
                    |   min_time|                                     0.1379683017730713|                                       0.12296414375305176|                                   0.2393805980682373|
                    |   max_time|                                      6.104769229888916|                                        0.1788628101348877|                                   0.3773176670074463|
                    |median_time|                                    0.17804932594299316|                                        0.1428384780883789|                                  0.26538968086242676|
                    +-----------+-------------------------------------------------------+----------------------------------------------------------+-----------------------------------------------------+

Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/
