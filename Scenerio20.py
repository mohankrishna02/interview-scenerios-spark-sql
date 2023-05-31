from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio20")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

df = spark.read.format("json").option("multiline", "true").load(
    "dbfs:/FileStore/flatjson/part-00000-tid-3675309499584050336-b8650962-dec3-4fe4-a204-c914090f019e-21-1-c000.json")
df.printSchema()
compdf = df.select(
    col("code"),
    col("commentCount"),
    col("createdAt"),
    col("description"),
    col("feedsComment"),
    col("id"),
    col("imagePaths"),
    col("images"),
    col("isdeleted"),
    col("lat"),
    struct(col("dislikes"), col("likes"), col("userAction")).alias("likeDislike"),
    col("lng"),
    col("location"),
    col("mediatype"),
    col("msg"),
    array(
        struct(
            col("createAt"),
            col("description"),
            col("id"),
            col("likeCount"),
            col("mediatype"),
            col("name"),
            col("place"),
            col("url")
        ).alias("element")
    ).alias("multiMedia"),
    col("name"),
    col("profilePicture"),
    col("title"),
    col("userId"),
    col("videoUrl"),
    col("totalFeed")
)

compdf.printSchema()
