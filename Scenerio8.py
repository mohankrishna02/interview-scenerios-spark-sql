from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio-8")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()
data = [
    ("India",),
    ("Pakistan",),
    ("SriLanka",)
]
myschema = ["teams"]
df = spark.createDataFrame(data, schema=myschema)
df.show()

# Through SQL
df.createOrReplaceTempView("crickettab")

# self join query for reference - select a.teams,b.teams from crickettab a inner join crickettab b on a.teams < b.teams

spark.sql(
    "select concat(a.teams, ' Vs ', b.teams) as matches from crickettab a inner join crickettab b on a.teams < b.teams").show()

# Through DSL

joindf = df.alias("a").join(df.alias("b"), col("a.teams") < col("b.teams"), "inner")
joindf.show()

finaldf = joindf.withColumn("matches", expr("concat(a.teams,' Vs ',b.teams)")).drop("teams", "teams").show()
