from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio-9")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()
data = [
    ("a", [1, 1, 1, 3]),
    ("b", [1, 2, 3, 4]),
    ("c", [1, 1, 1, 1, 4]),
    ("d", [3])
]
df = spark.createDataFrame(data, ["name", "rank"])
df.show()

explodedf = df.withColumn("rank", explode(col("rank")))
explodedf.show()

filtdf = explodedf.filter(col("rank") == 1)
filtdf.show()

countdf = filtdf.groupBy("name").agg(count("*").alias("count"))
countdf.show()

finaldf = countdf.select(col("name")).first()[0]
print(finaldf)
