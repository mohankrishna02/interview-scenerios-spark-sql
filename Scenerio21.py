from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio21")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

data = [
    ("SEA", "SF", 300),
    ("CHI", "SEA", 2000),
    ("SF", "SEA", 300),
    ("SEA", "CHI", 2000),
    ("SEA", "LND", 500),
    ("LND", "SEA", 500),
    ("LND", "CHI", 1000),
    ("CHI", "NDL", 180)]
df = spark.createDataFrame(data, ["from", "to", "dist"])
df.show()

# Through SQL
df.createOrReplaceTempView("trip")
spark.sql("""SELECT r1.from, r1.to, (r1.dist + r2.dist) AS roundtrip_dist
FROM trip r1
JOIN trip r2 ON r1.from = r2.to AND r1.to = r2.from
WHERE r1.from < r1.to
""").show()

# Through DSL
finaldf = df.alias("r1").join(df.alias("r2"),
                              (col("r1.from") == col("r2.to")) & (col("r1.to") == col("r2.from"))).where(
    col("r1.from") < col("r1.to")).select(col("r1.from"), col("r1.to"),
                                          (col("r1.dist") + col("r2.dist")).alias("roundtrip_dist"))

finaldf.show()
