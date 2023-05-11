from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio-5")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()
data1 = [
    (1, "abc", 31, "abc@gmail.com"),
    (2, "def", 23, "defyahoo.com"),
    (3, "xyz", 26, "xyz@gmail.com"),
    (4, "qwe", 34, "qwegmail.com"),
    (5, "iop", 24, "iop@gmail.com")
]
myschema1 = ["id", "name", "age", "email"]
df1 = spark.createDataFrame(data1, schema=myschema1)
df1.show()

data2 = [
    (11, "jkl", 22, "abc@gmail.com", 1000),
    (12, "vbn", 33, "vbn@yahoo.com", 3000),
    (13, "wer", 27, "wer", 2000),
    (14, "zxc", 30, "zxc.com", 2000),
    (15, "lkj", 29, "lkj@outlook.com", 2000)
]
myschema2 = ["id", "name", "age", "email", "salary"]
df2 = spark.createDataFrame(data2, schema=myschema2)
df2.show()

# number of partiion in df
partcount = df1.rdd.getNumPartitions()
print("Number of partition:- " + str(partcount))

df3 = df1.withColumn("salary", lit(1000))
df3.show()

# append df2 and df3, and form df4
df4 = df2.union(df3).orderBy(col("id"))
df4.show()

# Remove records which have invalid email from df4, emails with @ are considered to be valid.
rmdf = df4.filter(col("email").rlike("@"))
rmdf.show()

#Write df4 to a target location, by partitioning on salary.
rmdf.write.format("parquet").partitionBy("salary").save("D:/BigData/Processed Datasets/interdata")

