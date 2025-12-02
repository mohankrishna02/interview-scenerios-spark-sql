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
#############################################################################################
################solution 2 through DSL#######################################################

myschema=["orderid","statusdate" ,"status"]
df=spark.createDataFrame(data,schema=myschema)
print("======RAW DATA============")
df.show()

w = Window.partitionBy("orderid").orderBy("statusdate")

# Identify the first dispatched after Ordered
df2 = df.withColumn(
    "flag_start",
    when(
        (col("status") == "dispatched") &
        (lag("status").over(w) == "Ordered"),
        1
    ).otherwise(0)
)
#Spark does not have a direct case when function like SQL.
#Instead, when() + .otherwise() is the DSL equivalent of SQL CASE WHEN.

# Create a running group id for dispatched streak after Ordered
df3 = df2.withColumn(
    "grp",
    spark_sum("flag_start").over(w)
)
df3.show()

# Filter only the group where grp >= 1 and status = dispatched
result = df3.filter(
    (col("grp") >= 1) &
    (col("status") == "dispatched")
).select("orderid", "statusdate", "status")

result.show()

