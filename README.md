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

### Scenerio-5 
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

### Scenerio-6 (For Employee salary greater than 10000 give designation as manager else employee)
#### Input :-
```
+-----+----+------+
|empid|name|salary|
+-----+----+------+
|    1|   a| 10000|
|    2|   b|  5000|
|    3|   c| 15000|
|    4|   d| 25000|
|    5|   e| 50000|
|    6|   f|  7000|
+-----+----+------+
```
#### Expected Output :- 
```
+-----+----+------+-----------+
|empid|name|salary|Designation|
+-----+----+------+-----------+
|    1|   a| 10000|   Employee|
|    2|   b|  5000|   Employee|
|    3|   c| 15000|    Manager|
|    4|   d| 25000|    Manager|
|    5|   e| 50000|    Manager|
|    6|   f|  7000|   Employee|
+-----+----+------+-----------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio6.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio6.py>

### Scenerio-7
#### Input :- 
```
+-------+----------+----+--------+-----+
|sale_id|product_id|year|quantity|price|
+-------+----------+----+--------+-----+
|      1|       100|2010|      25| 5000|
|      2|       100|2011|      16| 5000|
|      3|       100|2012|       8| 5000|
|      4|       200|2010|      10| 9000|
|      5|       200|2011|      15| 9000|
|      6|       200|2012|      20| 7000|
|      7|       300|2010|      20| 7000|
|      8|       300|2011|      18| 7000|
|      9|       300|2012|      20| 7000|
+-------+----------+----+--------+-----+
```
#### Expected Output :- 
```
+-------+----------+----+--------+-----+
|sale_id|product_id|year|quantity|price|
+-------+----------+----+--------+-----+
|      6|       200|2012|      20| 7000|
|      9|       300|2012|      20| 7000|
|      1|       100|2010|      25| 5000|
|      8|       300|2011|      18| 7000|
+-------+----------+----+--------+-----+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio7.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio7.py>

### Scenerio-8
#### Input :- 
```
+--------+
|   teams|
+--------+
|   India|
|Pakistan|
|SriLanka|
+--------+
```
#### Expected Output :- 
```
+--------------------+
|             matches|
+--------------------+
|   India Vs Pakistan|
|   India Vs SriLanka|
|Pakistan Vs SriLanka|
+--------------------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio8.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio8.py>

### Scenerio-9 (write spark code, list of name of participants who has rank=1 most number of times)
#### Input :- 
```
+----+---------------+
|name|           rank|
+----+---------------+
|   a|   [1, 1, 1, 3]|
|   b|   [1, 2, 3, 4]|
|   c|[1, 1, 1, 1, 4]|
|   d|            [3]|
+----+---------------+
```
#### Expected Output :- 
```
c
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio9.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio9.py>

### Scenerio-10
#### Input :- 
```
+-----+-------------+-------------+
|empid|commissionamt|monthlastdate|
+-----+-------------+-------------+
|    1|          300|  31-Jan-2021|
|    1|          400|  28-Feb-2021|
|    1|          200|  31-Mar-2021|
|    2|         1000|  31-Oct-2021|
|    2|          900|  31-Dec-2021|
+-----+-------------+-------------+
```
#### Expected Output :- 
```
+-----+-------------+-------------+
|empid|commissionamt|monthlastdate|
+-----+-------------+-------------+
|    1|          200|  31-Mar-2021|
|    2|         1000|  31-Oct-2021|
+-----+-------------+-------------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio10.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio10.py>

### Scenerio-11 (I have a table called Emp_table, it has 3 columns, Emp name, emp ID , salary
in this I want to get salaries that are >10000 as Grade A, 5000-10000 as grade B and < 5000 as
Grade C, write an SQL query)
#### Input :- 
```
+------+---------------+------+
|emp_id|       emp_name|salary|
+------+---------------+------+
|     1|           Jhon|  4000|
|     2|      Tim David| 12000|
|     3|Json Bhrendroff|  7000|
|     4|         Jordon|  8000|
|     5|          Green| 14000|
|     6|         Brewis|  6000|
+------+---------------+------+
```
#### Expected Output :- 
```
+------+---------------+------+-----+
|emp_id|       emp_name|salary|grade|
+------+---------------+------+-----+
|     1|           Jhon|  4000|    C|
|     2|      Tim David| 12000|    A|
|     3|Json Bhrendroff|  7000|    B|
|     4|         Jordon|  8000|    B|
|     5|          Green| 14000|    A|
|     6|         Brewis|  6000|    B|
+------+---------------+------+-----+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio11.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio11.py>

### Scenerio-12
#### Input :- 
```
+--------------------+----------+
|               email|    mobile|
+--------------------+----------+
|Renuka1992@gmail.com|9856765434|
|anbu.arasu@gmail.com|9844567788|
+--------------------+----------+
```
#### Expected Output :- 
```
+--------------------+----------+
|               email|    mobile|
+--------------------+----------+
|R**********92@gma...|98*****434|
|a**********su@gma...|98*****788|
+--------------------+----------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio12.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio12.py>

## Scenerio-13 (We have employee id,employee name, department. Need count of every department employees.)
#### Input :- 
```
+------+--------+-----------+
|emp_id|emp_name|       dept|
+------+--------+-----------+
|     1|    Jhon|Development|
|     2|     Tim|Development|
|     3|   David|    Testing|
|     4|     Sam|    Testing|
|     5|   Green|    Testing|
|     6|  Miller| Production|
|     7|  Brevis| Production|
|     8|  Warner| Production|
|     9|    Salt| Production|
+------+--------+-----------+
```
#### Expected Output :- 
```
+-----------+-----+
|       dept|total|
+-----------+-----+
|Development|    2|
|    Testing|    3|
| Production|    4|
+-----------+-----+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio13.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio13.py>

## Scenerio-14 (We need total marks)
#### Input :- 
```
+------+------+------+-------+-----+-------+------+
|rollno|  name|telugu|english|maths|science|social|
+------+------+------+-------+-----+-------+------+
|203040|rajesh|    10|     20|   30|     40|    50|
+------+------+------+-------+-----+-------+------+
```
#### Expected Output :- 
```
+------+------+------+-------+-----+-------+------+-----+
|rollno|  name|telugu|english|maths|science|social|total|
+------+------+------+-------+-----+-------+------+-----+
|203040|rajesh|    10|     20|   30|     40|    50|  150|
+------+------+------+-------+-----+-------+------+-----+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio14.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio14.py>

## Scenerio-15 (Extend and Append list in  python and scala)

#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio15.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio15.py>
