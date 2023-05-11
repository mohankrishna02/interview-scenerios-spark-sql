## Spark and SQL Interview Scenerio Questions
### Scenerio-1 (Query to get who are getting equal salary)
#### Input :-
```
+--------+---------+--------+------+-------------------+------+
|workerid|firstname|lastname|salary|        joiningdate|depart|
+--------+---------+--------+------+-------------------+------+
|     001|   Monika|   Arora|100000|2014-02-20 09:00:00|    HR|
|     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
|     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
|     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
|     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
+--------+---------+--------+------+-------------------+------+
```
#### Expected Output :- 
```
+--------+---------+--------+------+-------------------+------+
|workerid|firstname|lastname|salary|        joiningdate|depart|
+--------+---------+--------+------+-------------------+------+
|     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
|     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
|     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
|     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
+--------+---------+--------+------+-------------------+------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenarios-spark-sql/blob/master/src/pack/Scenerio1.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenarios-spark-sql/blob/master/Scenerio-1.py>

### Scenerio-2 (Need the dates when the status gets changed like ordered to dispatched)
#### Input :- 
```
+-------+----------+----------+
|orderid|statusdate|    status|
+-------+----------+----------+
|      1|     1-Jan|   Ordered|
|      1|     2-Jan|dispatched|
|      1|     3-Jan|dispatched|
|      1|     4-Jan|   Shipped|
|      1|     5-Jan|   Shipped|
|      1|     6-Jan| Delivered|
|      2|     1-Jan|   Ordered|
|      2|     2-Jan|dispatched|
|      2|     3-Jan|   shipped|
+-------+----------+----------+
```
#### Expected Output :- 
```
+-------+----------+----------+
|orderid|statusdate|    status|
+-------+----------+----------+
|      1|     2-Jan|dispatched|
|      1|     3-Jan|dispatched|
|      2|     2-Jan|dispatched|
+-------+----------+----------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenarios-spark-sql/blob/master/src/pack/Scenerio2.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenarios-spark-sql/blob/master/Scenerio2.py>

### Scenerio-3 
#### Input :- 
```
+--------+----------+------+
|sensorid| timestamp|values|
+--------+----------+------+
|    1111|2021-01-15|    10|
|    1111|2021-01-16|    15|
|    1111|2021-01-17|    30|
|    1112|2021-01-15|    10|
|    1112|2021-01-15|    20|
|    1112|2021-01-15|    30|
+--------+----------+------+
```
#### Expected Output :- 
```
+--------+----------+------+
|sensorid| timestamp|values|
+--------+----------+------+
|    1111|2021-01-15|     5|
|    1111|2021-01-16|    15|
|    1112|2021-01-15|    10|
|    1112|2021-01-15|    10|
+--------+----------+------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio3.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio3.py>

### Scenerio-4 (Write a query to list the unique customer names in the custtab table, along with the number of addresses associated with each customer.)
#### Input :- 
```
+------+-----------+-------+
|custid|   custname|address|
+------+-----------+-------+
|     1|   Mark Ray|     AB|
|     2|Peter Smith|     CD|
|     1|   Mark Ray|     EF|
|     2|Peter Smith|     GH|
|     2|Peter Smith|     CD|
|     3|       Kate|     IJ|
+------+-----------+-------+
```
#### Expected Output :- 
```
+------+-----------+--------+
|custid|   custname| address|
+------+-----------+--------+
|     1|   Mark Ray|[EF, AB]|
|     2|Peter Smith|[CD, GH]|
|     3|       Kate|    [IJ]|
+------+-----------+--------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio4.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio4.py>

### Scenerio-4 
* Read data from above file into dataframes(df1 and df2).
* Display number of partitions in df1.
* Create a new dataframe df3 from df1, along with a new column salary, and keep it constant 1000
* append df2 and df3, and form df4
* Remove records which have invalid email from df4, emails with @ are considered to be valid.
* Write df4 to a target location, by partitioning on salary.
#### Input :- 
```
+---+----+---+-------------+
| id|name|age|        email|
+---+----+---+-------------+
|  1| abc| 31|abc@gmail.com|
|  2| def| 23| defyahoo.com|
|  3| xyz| 26|xyz@gmail.com|
|  4| qwe| 34| qwegmail.com|
|  5| iop| 24|iop@gmail.com|
+---+----+---+-------------+
```
```
+---+----+---+---------------+------+
| id|name|age|          email|salary|
+---+----+---+---------------+------+
| 11| jkl| 22|  abc@gmail.com|  1000|
| 12| vbn| 33|  vbn@yahoo.com|  3000|
| 13| wer| 27|            wer|  2000|
| 14| zxc| 30|        zxc.com|  2000|
| 15| lkj| 29|lkj@outlook.com|  2000|
+---+----+---+---------------+------+
```
#### Expected Output :- 
```
+---+----+---+---------------+------+
| id|name|age|          email|salary|
+---+----+---+---------------+------+
|  1| abc| 31|  abc@gmail.com|  1000|
|  3| xyz| 26|  xyz@gmail.com|  1000|
|  5| iop| 24|  iop@gmail.com|  1000|
| 11| jkl| 22|  abc@gmail.com|  1000|
| 12| vbn| 33|  vbn@yahoo.com|  3000|
| 15| lkj| 29|lkj@outlook.com|  2000|
+---+----+---+---------------+------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio5.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio5.py>
