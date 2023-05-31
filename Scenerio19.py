from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio19")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

df = spark.read.format("json").option("multiline", "true").load("dbfs:/FileStore/scen.json")
df.printSchema()
finaldf = df.withColumn("multiMedia", explode(col("multiMedia"))).withColumn("dislikes",
                                                                             expr("likeDislike.dislikes")).withColumn(
    "likes", expr("likeDislike.likes")).withColumn("userAction", expr("likeDislike.userAction")).withColumn("createAt",
                                                                                                            expr(
                                                                                                                "multiMedia.createAt")).withColumn(
    "description", expr("multiMedia.description")).withColumn("id", expr("multiMedia.id")).withColumn("likeCount", expr(
    "multiMedia.likeCount")).withColumn("mediatype", expr("multiMedia.mediatype")).withColumn("name", expr(
    "multiMedia.name")).withColumn("place", expr("multiMedia.place")).withColumn("url", expr("multiMedia.url")).drop(
    "likeDislike", "multiMedia")
print("flat Schema")
finaldf.printSchema()
finaldf.show()
