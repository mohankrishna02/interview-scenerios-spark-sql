from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio25")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").option("header", "true") \
    .option("mode", "DROPMALFORMED") \
    .load("D:/BigData/Datasets/Scenerio25.csv")
df.show()
