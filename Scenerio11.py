from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio-10")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()
data = [
    (1, "Jhon", 4000),
    (2, "Tim David", 12000),
    (3, "Json Bhrendroff", 7000),
    (4, "Jordon", 8000),
    (5, "Green", 14000),
    (6, "Brewis", 6000)
]
df = spark.createDataFrame(data, ["emp_id", "emp_name", "salary"])
df.show()

# Through SQL
df.createOrReplaceTempView("emptab")
spark.sql(
    "select *,case when salary<5000 then 'C' when salary between 5000 and 10000 then 'B' else 'A' end as grade from emptab ").show()

# Through DSL
finaldf = df.withColumn("grade", expr(
    "case when salary<5000 then 'C' when salary between 5000 and 10000 then 'B' else 'A' end")).show()
