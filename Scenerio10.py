from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio-10")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()
data = [
    (1, 300, "31-Jan-2021"),
    (1, 400, "28-Feb-2021"),
    (1, 200, "31-Mar-2021"),
    (2, 1000, "31-Oct-2021"),
    (2, 900, "31-Dec-2021")
]
df = spark.createDataFrame(data, ["empid", "commissionamt", "monthlastdate"])
df.show()

df = spark.createDataFrame(data, ["empid", "commissionamt", "monthlastdate"])
df.show()

df =df.withColumn("date",to_date(df["monthlastdate"],"dd-MMM-yyyy"))
df.show()

win = Window.partitionBy("empid").orderBy(col("date").desc())
df_wrk = df.withColumn("row_num",row_number().over(win))
res_df = df_wrk.filter(col("row_num")==1).drop("row_number")
res_df.show()
