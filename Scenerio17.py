from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio17")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()
data = [(1, "Tim", 24, "Kerala", "India"),
        (2, "Asman", 26, "Kerala", "India")]
df1 = spark.createDataFrame(data, ["emp_id", "name", "age", "state", "country"])
df1.show()

data2 = [(1, "Tim", 24, "Comcity"),
         (2, "Asman", 26, "bimcity")]
df2 = spark.createDataFrame(data2, ["emp_id", "name", "age", "address"])
df2.show()

findf = df1.join(df2, ["emp_id", "name", "age"], "outer")
findf.show()
