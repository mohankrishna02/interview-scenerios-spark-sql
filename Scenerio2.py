from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
conf = SparkConf().setMaster("local[*]").setAppName("Scenerio-2")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()
data = [
      (1, "1-Jan", "Ordered"),
      (1, "2-Jan", "dispatched"),
      (1, "3-Jan", "dispatched"),
      (1, "4-Jan", "Shipped"),
      (1, "5-Jan", "Shipped"),
      (1, "6-Jan", "Delivered"),
      (2, "1-Jan", "Ordered"),
      (2, "2-Jan", "dispatched"),
      (2, "3-Jan", "shipped")]
myschema = ["orderid","statusdate","status"]
df = spark.createDataFrame(data,schema=myschema)
df.show()
#Through SQL
df.createOrReplaceTempView("ordertab")
spark.sql("select * from ordertab where status = 'dispatched' and orderid in(select orderid from ordertab where status = 'Ordered')").show()

#Through DSL
result = df.filter(
    (col("status") == "dispatched") &
    (col("orderid").isin(
        *[row[0] for row in df.filter(col("status") == "Ordered").select("orderid").collect()]
    ))
)
result.show()
