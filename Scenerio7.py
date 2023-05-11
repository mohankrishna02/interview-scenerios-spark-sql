from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio-7")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()
data = [
    (1, 100, 2010, 25, 5000),
    (2, 100, 2011, 16, 5000),
    (3, 100, 2012, 8, 5000),
    (4, 200, 2010, 10, 9000),
    (5, 200, 2011, 15, 9000),
    (6, 200, 2012, 20, 7000),
    (7, 300, 2010, 20, 7000),
    (8, 300, 2011, 18, 7000),
    (9, 300, 2012, 20, 7000)
]
myschema = ["sale_id", "product_id", "year", "quantity", "price"]
df = spark.createDataFrame(data, schema=myschema)
df.show()

#Through SQL
df.createOrReplaceTempView("salestab")
spark.sql("SELECT *FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY year ORDER BY quantity DESC) AS rank FROM salestab) AS rankdf WHERE rank = 1 ORDER BY sale_id").show()

#Through DSL
win = Window.partitionBy("year").orderBy(col("quantity").desc())

rankdf = df.withColumn("rank", dense_rank().over(win))
rankdf.show()

finaldf = rankdf.filter(col("rank") == 1).drop("rank").orderBy("sale_id").show()
