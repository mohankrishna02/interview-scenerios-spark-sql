from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark import *

conf = SparkConf().setMaster("local[*]").setAppName("Scenerio22")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

data = [(1, "26-May", 100),
        (1, "27-May", 200),
        (1, "28-May", 300),
        (2, "29-May", 400),
        (3, "30-May", 500),
        (3, "31-May", 600)]
df = spark.createDataFrame(data, ["pid", "date", "price"])
df.show()
# Through SQL
df.createOrReplaceTempView("ordertab")
spark.sql("select pid,date,price, sum(price) over(partition by(pid) order by(price)) as new_price from ordertab").show()
# Through DSL
wn = Window.partitionBy("pid").orderBy("price")
finaldf = df.withColumn("new_price", sum("price").
                        over(wn)).show()
