from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio21")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

data = [
    ("SEA", "SF", 300),
    ("CHI", "SEA", 2000),
    ("SF", "SEA", 300),
    ("SEA", "CHI", 2000),
    ("SEA", "LND", 500),
    ("LND", "SEA", 500),
    ("LND", "CHI", 1000),
    ("CHI", "NDL", 180)]
df = spark.createDataFrame(data, ["from", "to", "dist"])
df.show()

# Through SQL
df.createOrReplaceTempView("trip")
spark.sql(
    "select a.from,a.to,(a.dist+b.dist) as total_dist from trip a join trip b on a.from=b.to and a.to=b.from").show()

# Through DSL
finaldf = df.alias("a").join(df.alias("b"), (col("a.from") == col("b.to")) & (col("a.to") == col("b.from"))).select(
    col("a.from"), col("a.to"), (col("a.dist") + col("b.dist")).alias("total_dist"))
finaldf.show()
