from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("test")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

data = [("A", "AA"), ("B", "BB"), ("C", "CC"), ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]

df = spark.createDataFrame(data, ["child", "parent"])
df.show()

joindf = df.alias("a").join(df.alias("b"), col("a.child") == col("b.parent")).select(
    col("a.child").alias("child_a"),
    col("a.parent").alias("parent_a"),
    col("b.child").alias("child_b"),
    col("b.parent").alias("parent_b")
)
joindf.show()

findf = joindf.withColumnRenamed("child_a", "parent").withColumnRenamed("parent_a", "grandparent").withColumnRenamed(
    "child_b", "child").drop("parent_b").select("child", "parent", "grandparent")

findf.show()

# another way

df2 = df.withColumnRenamed("child", "child1").withColumnRenamed("parent", "parent1")
df2.show()

secondjoindf = df.join(df2, col("parent") == col("child1"), "inner")
secondjoindf.show()

finaldf = secondjoindf.withColumnRenamed("parent1", "grandparent").drop("child1")
finaldf.show()
