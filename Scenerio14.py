from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio14")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

data = [
    (203040, "rajesh", 10, 20, 30, 40, 50)
]

df = spark.createDataFrame(data, ["rollno", "name", "telugu", "english", "maths", "science", "social"])
df.show()

# Through SQL
df.createOrReplaceTempView("marks")
spark.sql("select *, (telugu+english+maths+science+social) as total from marks").show()

# Through DSL
finaldf = df.withColumn("total", expr("telugu+english+maths+science+social")).show()
