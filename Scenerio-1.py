from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
conf = SparkConf().setMaster("local[*]").setAppName("Scenerio-1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

data = [("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")]
myschema = ["workerid","firstname","lastname","salary","joiningdate","depart"]
df = spark.createDataFrame(data,schema=myschema)
df.show()
#Through SQL
df.createOrReplaceTempView("worktab")
spark.sql("select a.workerid,a.firstname,a.lastname,a.salary,a.joiningdate,a.depart from worktab a, worktab b where a.salary=b.salary and a.workerid !=b.workerid").show()

#Through Spark DSL
finaldf = df.alias("a").join(df.alias("b"), (col("a.salary") == col("b.salary")) & (col("a.workerid") != col("b.workerid")), "inner").select(col("a.workerid"), col("a.firstname"), col("a.lastname"), col("a.salary"), col("a.joiningdate"), col("a.depart")).show()
