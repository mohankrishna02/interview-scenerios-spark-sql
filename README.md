## Spark and SQL Interview Scenerio Questions

### Table of Contents

|No| Scenerios                                                                |
|--|--------------------------------------------------------------------------|
|1 |[Scenerio-1](#scenerio-1)                                                 |
|2 |[Scenerio-2](#scenerio-2)                                                 |
|3 |[Scenerio-3](#scenerio-3)                                                 |
|4 |[Scenerio-4](#scenerio-4)                                                 |
|5 |[Scenerio-5](#scenerio-5)                                                 |
|6 |[Scenerio-6](#scenerio-6)                                                 |
|7 |[Scenerio-7](#scenerio-7)                                                 |
|8 |[Scenerio-8](#scenerio-8)                                                 |
|9 |[Scenerio-9](#scenerio-9)                                                 |
|10|[Scenerio-10](#scenerio-10)                                               |
|11|[Scenerio-11](#scenerio-11)                                               |
|12|[Scenerio-12](#scenerio-12)                                               |
|13|[Scenerio-13](#scenerio-13)                                               |
|14|[Scenerio-14](#scenerio-14)                                               |
|15|[Scenerio-15](#scenerio-15)                                               |
|16|[Scenerio-16](#scenerio-16)                                               |
|17|[Scenerio-17](#scenerio-17)                                               |
|18|[Scenerio-18](#scenerio-18)                                               |
|19|[Scenerio-19](#scenerio-19)                                               |
|20|[Scenerio-20](#scenerio-20)                                               |
|21|[Scenerio-21](#scenerio-21)                                               |
|22|[Scenerio-22](#scenerio-22)                                               |
|23|[Scenerio-23](#scenerio-23)                                               |
|24|[Scenerio-24](#scenerio-24)                                               |

### Scenerio-1 
#### Query to get who are getting equal salary
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

**[⬆ Back to Top](#table-of-contents)**

### Scenerio-2 
#### (Need the dates when the status gets changed like ordered to dispatched)
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

**[⬆ Back to Top](#table-of-contents)**

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

**[⬆ Back to Top](#table-of-contents)**

### Scenerio-4 
#### (Write a query to list the unique customer names in the custtab table, along with the number of addresses associated with each customer.)
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

**[⬆ Back to Top](#table-of-contents)**

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

**[⬆ Back to Top](#table-of-contents)**

### Scenerio-6 
#### (For Employee salary greater than 10000 give designation as manager else employee)
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

**[⬆ Back to Top](#table-of-contents)**

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

**[⬆ Back to Top](#table-of-contents)**

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

**[⬆ Back to Top](#table-of-contents)**

### Scenerio-9 
#### (write spark code, list of name of participants who has rank=1 most number of times)
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

**[⬆ Back to Top](#table-of-contents)**

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

**[⬆ Back to Top](#table-of-contents)**

### Scenerio-11 
#### (I have a table called Emp_table, it has 3 columns, Emp name, emp ID , salary
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

**[⬆ Back to Top](#table-of-contents)**

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

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-13
####  (We have employee id,employee name, department. Need count of every department employees.)
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

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-14 
#### (We need total marks)
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

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-15 
#### (Extend and Append list in  python and scala)
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio15.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio15.py>

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-16 
#### (Remove duplicates)
#### Input :- 
```
+---+----+-----------+------+
| id|name|       dept|salary|
+---+----+-----------+------+
|  1|Jhon|    Testing|  5000|
|  2| Tim|Development|  6000|
|  3|Jhon|Development|  5000|
|  4| Sky| Prodcution|  8000|
+---+----+-----------+------+
```
#### Expected Output :- 
```
+---+----+-----------+------+
| id|name|       dept|salary|
+---+----+-----------+------+
|  1|Jhon|    Testing|  5000|
|  2| Tim|Development|  6000|
|  4| Sky| Prodcution|  8000|
+---+----+-----------+------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio16.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio16.py>

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-17 
#### (df1 contains Employeeid,Name,Age,State,Country columns df2 contains Employeeid,Name,Age,Address columns. how do you merge df1 and df2 to get the following output Employeeid,Name,Age,State,Country,Address)
#### Input :- 
```
+------+-----+---+------+-------+          
|emp_id| name|age| state|country|
+------+-----+---+------+-------+
|     1|  Tim| 24|Kerala|  India|
|     2|Asman| 26|Kerala|  India|
+------+-----+---+------+-------+
```
```
+------+-----+---+-------+
|emp_id| name|age|address|
+------+-----+---+-------+
|     1|  Tim| 24|Comcity|
|     2|Asman| 26|bimcity|
+------+-----+---+-------+
```
#### Expected Output :- 
```
+------+-----+---+------+-------+-------+
|emp_id| name|age| state|country|address|
+------+-----+---+------+-------+-------+
|     1|  Tim| 24|Kerala|  India|Comcity|
|     2|Asman| 26|Kerala|  India|bimcity|
+------+-----+---+------+-------+-------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio17.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio17.py>

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-18
#### Input :- 
```
+------------------+
|              word|
+------------------+
|The Social Dilemma|
+------------------+
```

#### Expected Output :- 
```
+------------------+
|      reverse word|
+------------------+
|ehT laicoS ammeliD|
+------------------+
```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio18.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio18.py>

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-19 
#### (Flatten the below complex dataframe)
#### Input :- 
```
root
 |-- code: long (nullable = true)
 |-- commentCount: long (nullable = true)
 |-- createdAt: string (nullable = true)
 |-- description: string (nullable = true)
 |-- feedsComment: string (nullable = true)
 |-- id: long (nullable = true)
 |-- imagePaths: string (nullable = true)
 |-- images: string (nullable = true)
 |-- isdeleted: boolean (nullable = true)
 |-- lat: long (nullable = true)
 |-- likeDislike: struct (nullable = true)
 |    |-- dislikes: long (nullable = true)
 |    |-- likes: long (nullable = true)
 |    |-- userAction: long (nullable = true)
 |-- lng: long (nullable = true)
 |-- location: string (nullable = true)
 |-- mediatype: long (nullable = true)
 |-- msg: string (nullable = true)
 |-- multiMedia: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- createAt: string (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- likeCount: long (nullable = true)
 |    |    |-- mediatype: long (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- place: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |-- name: string (nullable = true)
 |-- profilePicture: string (nullable = true)
 |-- title: string (nullable = true)
 |-- totalFeed: long (nullable = true)
 |-- userId: long (nullable = true)
 |-- videoUrl: string (nullable = true)
```

#### Expected Output :- 
```
root
 |-- code: long (nullable = true)
 |-- commentCount: long (nullable = true)
 |-- createdAt: string (nullable = true)
 |-- description: string (nullable = true)
 |-- feedsComment: string (nullable = true)
 |-- id: long (nullable = true)
 |-- imagePaths: string (nullable = true)
 |-- images: string (nullable = true)
 |-- isdeleted: boolean (nullable = true)
 |-- lat: long (nullable = true)
 |-- lng: long (nullable = true)
 |-- location: string (nullable = true)
 |-- mediatype: long (nullable = true)
 |-- msg: string (nullable = true)
 |-- name: string (nullable = true)
 |-- profilePicture: string (nullable = true)
 |-- title: string (nullable = true)
 |-- totalFeed: long (nullable = true)
 |-- userId: long (nullable = true)
 |-- videoUrl: string (nullable = true)
 |-- dislikes: long (nullable = true)
 |-- likes: long (nullable = true)
 |-- userAction: long (nullable = true)
 |-- createAt: string (nullable = true)
 |-- likeCount: long (nullable = true)
 |-- place: string (nullable = true)
 |-- url: string (nullable = true)
```
#### Solution :- 
Dataset - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Datasets/scen.json> <br>
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio19.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio19.py>

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-20 
#### (Generate the complex dataframe)
#### Input :- 
```
root
 |-- code: long (nullable = true)
 |-- commentCount: long (nullable = true)
 |-- createAt: string (nullable = true)
 |-- createdAt: string (nullable = true)
 |-- description: string (nullable = true)
 |-- dislikes: long (nullable = true)
 |-- feedsComment: string (nullable = true)
 |-- id: long (nullable = true)
 |-- imagePaths: string (nullable = true)
 |-- images: string (nullable = true)
 |-- isdeleted: boolean (nullable = true)
 |-- lat: long (nullable = true)
 |-- likeCount: long (nullable = true)
 |-- likes: long (nullable = true)
 |-- lng: long (nullable = true)
 |-- location: string (nullable = true)
 |-- mediatype: long (nullable = true)
 |-- msg: string (nullable = true)
 |-- name: string (nullable = true)
 |-- place: string (nullable = true)
 |-- profilePicture: string (nullable = true)
 |-- title: string (nullable = true)
 |-- totalFeed: long (nullable = true)
 |-- url: string (nullable = true)
 |-- userAction: long (nullable = true)
 |-- userId: long (nullable = true)
 |-- videoUrl: string (nullable = true)
```

#### Expected Output :- 
```
root
 |-- code: long (nullable = true)
 |-- commentCount: long (nullable = true)
 |-- createdAt: string (nullable = true)
 |-- description: string (nullable = true)
 |-- feedsComment: string (nullable = true)
 |-- id: long (nullable = true)
 |-- imagePaths: string (nullable = true)
 |-- images: string (nullable = true)
 |-- isdeleted: boolean (nullable = true)
 |-- lat: long (nullable = true)
 |-- likeDislike: struct (nullable = false)
 |    |-- dislikes: long (nullable = true)
 |    |-- likes: long (nullable = true)
 |    |-- userAction: long (nullable = true)
 |-- lng: long (nullable = true)
 |-- location: string (nullable = true)
 |-- mediatype: long (nullable = true)
 |-- msg: string (nullable = true)
 |-- multiMedia: array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- createAt: string (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- likeCount: long (nullable = true)
 |    |    |-- mediatype: long (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- place: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |-- name: string (nullable = true)
 |-- profilePicture: string (nullable = true)
 |-- title: string (nullable = true)
 |-- userId: long (nullable = true)
 |-- videoUrl: string (nullable = true)
 |-- totalFeed: long (nullable = true)
```
#### Solution :- 
Dataset - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Datasets/scen20.json> <br>
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio20.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio20.py>

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-21 
#### (The roundtrip distance should be calculated using spark or SQL.)
#### Input :- 
```
+----+---+----+
|from| to|dist|
+----+---+----+
| SEA| SF| 300|
| CHI|SEA|2000|
|  SF|SEA| 300|
| SEA|CHI|2000|
| SEA|LND| 500|
| LND|SEA| 500|
| LND|CHI|1000|
| CHI|NDL| 180|
+----+---+----+
```

#### Expected Output :- 
```
+----+---+--------------+
|from| to|roundtrip_dist|
+----+---+--------------+
| SEA| SF|           600|
| CHI|SEA|          4000|
| LND|SEA|          1000|
+----+---+--------------+

```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio21.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio21.py>

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-22 
#### (Cumilative sum)
#### Input :- 
```
+---+------+-----+
|pid|  date|price|
+---+------+-----+
|  1|26-May|  100|
|  1|27-May|  200|
|  1|28-May|  300|
|  2|29-May|  400|
|  3|30-May|  500|
|  3|31-May|  600|
+---+------+-----+
```

#### Expected Output :- 
```
+---+------+-----+---------+
|pid|  date|price|new_price|
+---+------+-----+---------+
|  1|26-May|  100|      100|
|  1|27-May|  200|      300|
|  1|28-May|  300|      600|
|  2|29-May|  400|      400|
|  3|30-May|  500|      500|
|  3|31-May|  600|     1100|
+---+------+-----+---------+

```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio22.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio22.py>

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-23 
#### Input :- 
```
+-----------+-----------+
|customer_id|product_key|
+-----------+-----------+
|          1|          5|
|          2|          6|
|          3|          5|
|          3|          6|
|          1|          6|
+-----------+-----------+
```
```
+-----------+
|product_key|
+-----------+
|          5|
|          6|
+-----------+

```

#### Expected Output :- 
```
+-----------+
|customer_id|
+-----------+
|          1|
|          3|
+-----------+

```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio23.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio23.py>

**[⬆ Back to Top](#table-of-contents)**

## Scenerio-24 
#### Input :- 
```
+------+------------+
|userid|        page|
+------+------------+
|     1|        home|
|     1|    products|
|     1|    checkout|
|     1|confirmation|
|     2|        home|
|     2|    products|
|     2|        cart|
|     2|    checkout|
|     2|confirmation|
|     2|        home|
|     2|    products|
+------+------------+

```

#### Expected Output :- 
```
+------+--------------------------------------------------------------+
|userid|pages                                                         |
+------+--------------------------------------------------------------+
|1     |[home, products, checkout, confirmation]                      |
|2     |[home, products, cart, checkout, confirmation, home, products]|
+------+--------------------------------------------------------------+

```
#### Solution :- 
Scala-Spark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/src/pack/Scenerio24.scala> <br>
PySpark - <https://github.com/mohankrishna02/interview-scenerios-spark-sql/blob/master/Scenerio24.py>

**[⬆ Back to Top](#table-of-contents)**

