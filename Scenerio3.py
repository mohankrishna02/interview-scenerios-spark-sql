from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio-3")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()
data = [(1111, "2021-01-15", 10),
        (1111, "2021-01-16", 15),
        (1111, "2021-01-17", 30),
        (1112, "2021-01-15", 10),
        (1112, "2021-01-15", 20),
        (1112, "2021-01-15", 30)]

myschema = ["sensorid", "timestamp", "values"]

df = spark.createDataFrame(data, schema=myschema)
df.show()

d1 = Window.partitionBy("sensorid").orderBy("values")

finaldf = df.withColumn("nextvalues", lead("values", 1).over(d1)) \
    .filter(col("nextvalues").isNotNull()) \
    .withColumn("values", expr("nextvalues-values")) \
    .drop("nextvalues") \
    .orderBy(col("sensorid")).show()
