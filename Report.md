# Lab 3: Spark and Parquet Optimization Report

Name:
 
NetID: 

## Part 1: Spark

#### Question 1: 
How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)`?

Code:
```SQL

<your_query_here>

```


Output:
```

<copy_resulting_table_from_stdout_here>

```


#### Question 2: 
How would you express the following using the object interface instead of SQL: `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`?

Code:
```python

<your_Spark_Transformations_here>

```


Output:
```

<copy_resulting_table_from_stdout_here>

```

#### Question 3: 
Using a single SQL query, how many distinct boats did each sailor reserve? 
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats. 
(Hint: you may need to use `first(...)` aggregation function on some columns.) 
Provide both your query and the resulting DataFrame in your response to this question.

Code:
```SQL

<your_SQL_query_here>

```


Output:
```

<copy_resulting_table_from_stdout_here>

```

#### Question 4: 
Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID).
  What are the results for the ten terms with the shortest *average* track durations?
  Include both your query code and resulting DataFrame in your response.


Code:
```python

<your_Spark_Transformations_here>

```


Output:
```

<copy_resulting_table_from_stdout_here>

```
#### Question 5: 
Create a query using Spark transformations that finds the number of distinct tracks associated (through artistID) to each term.
  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
  Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response. 


## Part 2: Parquet Optimization:

What to include in your report:
  - Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
  - How do the results in parts 2.3, 2.4, and 2.5 compare?
  - What did you try in part 2.5 to improve performance for each query?
  - What worked, and what didn't work?

#### csv_sum_orders.py
- Small dataset
  ``` Results
  Times to run 'csv_sum_orders' Query 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv
  Maximum Time :6.238958835601807
  minimum Time :0.23334908485412598
  median Time :0.35582447052001953
  ```
- Moderate dataset
  ```Results
  Times to run 'csv_sum_orders' Query 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv
  Maximum Time :0.8827781677246094
  minimum Time :0.5491855144500732
  median Time :0.6053314208984375
  ```

- Big datasets
  ```Results
  Times to run 'csv_sum_orders' Query 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv
  Maximum Time :27.832977294921875
  minimum Time :16.538712739944458
  median Time :22.434118509292603
  ```

#### csv_big_spender.py
- Small dataset
  ``` Results
  Times to run 'csv_big_spender' Query 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv
  Maximum Time :5.899007558822632
  minimum Time :0.14420795440673828
  median Time :0.1874547004699707
  ```
- Moderate dataset
  ```Results
  Times to run 'csv_big_spender' Query 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv
  Maximum Time :3.4721014499664307
  minimum Time :0.25869154930114746
  median Time :0.30533385276794434
  ```

- Big datasets
  ```Results
  Times to run 'csv_big_spender' Query 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv
  Maximum Time :17.344022750854492
  minimum Time :12.413119554519653
  median Time :15.028894424438477
  ```

#### csv_brian.py
- Small dataset
  ``` Results
  Times to run csv_brian 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv:
  {'min_time': 0.1459357738494873, 'max_time': 5.9472692012786865, 'median_time': 0.18015336990356445}
  ```
- Moderate dataset
  ```Results
  Times to run csv_brian 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv:
  {'min_time': 0.29993534088134766, 'max_time': 3.8426125049591064, 'median_time': 0.3494904041290283}
  ```

- Big datasets
  ```Results
  Times to run csv_brian 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv:
  {'min_time': 13.365129709243774, 'max_time': 18.120110750198364, 'median_time': 15.940354347229004}
  ```

#### pq_sum_orders.py
- Small dataset
  ``` Results
  Times to run 'pq_sum_orders' Query 25 times on hdfs:/user/hl5679_nyu_edu/peopleSmall.parquet
  Maximum Time :5.886028289794922
  minimum Time :0.22558093070983887
  median Time :0.29825830459594727
  ```
- Moderate dataset
  ```Results
  Times to run 'pq_sum_orders' Query 25 times on hdfs:/user/hl5679_nyu_edu/peopleModerate.parquet
  Maximum Time :0.9113643169403076
  minimum Time :0.3890421390533447
  median Time :0.5122823715209961
  ```

- Big datasets
  ```Results
  Times to run 'pq_sum_orders' Query 25 times on hdfs:/user/hl5679_nyu_edu/peopleBig.parquet
  Maximum Time :6.12724232673645
  minimum Time :4.598937749862671
  median Time :4.872447967529297
  ```

#### pq_big_spender.py
- Small dataset
  ``` Results
  Times to run 'pq_big_spender' Query 25 times on hdfs:/user/hl5679_nyu_edu/peopleSmall.parquet
  Maximum Time :2.371960401535034
  minimum Time :0.12933969497680664
  median Time :0.17449498176574707
  ```
- Moderate dataset
  ```Results
  Times to run 'pq_big_spender' Query 25 times on hdfs:/user/hl5679_nyu_edu/peopleModerate.parquet
  Maximum Time :0.3303830623626709
  minimum Time :0.12624192237854004
  median Time :0.1600501537322998
  ```

- Big datasets
  ```Results
  Times to run 'pq_big_spender' Query 25 times on hdfs:/user/hl5679_nyu_edu/peopleBig.parquet
  Maximum Time :4.014336824417114
  minimum Time :3.76212739944458
  median Time :3.8945958614349365
  ```

#### pq_brian.py
- Small dataset
  ``` Results
  Times to run 'pq_brian' Query 25 times on hdfs:/user/hl5679_nyu_edu/peopleSmall.parquet
  Maximum Time :5.11160135269165
  minimum Time :0.13640475273132324
  median Time :0.18003582954406738
  ```
- Moderate dataset
  ```Results
  Times to run 'pq_brian' Query 25 times on hdfs:/user/hl5679_nyu_edu/peopleModerate.parquet
  Maximum Time :0.4647061824798584
  minimum Time :0.12632107734680176
  median Time :0.14973711967468262
  ```

- Big datasets
  ```Results
  Times to run 'pq_brian' Query 25 times on hdfs:/user/hl5679_nyu_edu/peopleBig.parquet
  Maximum Time :4.596149444580078
  minimum Time :0.9128859043121338
  median Time :0.9443416595458984
  ```

Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/
