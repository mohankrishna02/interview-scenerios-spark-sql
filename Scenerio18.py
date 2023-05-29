from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio18")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# Create input DataFrame
inputdf = spark.createDataFrame([("The Social Dilemma",)], ["word"])
inputdf.show()

# Define UDF for reversing words
def reverse_sentence(sentence):
    return " ".join([word[::-1] for word in sentence.split(" ")])

# Register UDF
reverse_udf = udf(reverse_sentence, StringType())

# Apply UDF to input DataFrame
outputdf = inputdf.withColumn("reverse word", reverse_udf("word")).drop("word")
outputdf.show()