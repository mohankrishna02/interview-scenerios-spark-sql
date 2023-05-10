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
