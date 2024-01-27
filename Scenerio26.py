from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

conf = SparkConf().setMaster("local[*]").setAppName("test")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

sourcedata = [
      (1, "A"),
      (2, "B"),
      (3, "C"),
      (4, "D")]
mysourceshcema = ["id","name"]
sourcedf = spark.createDataFrame(sourcedata,schema=mysourceshcema)
sourcedf.show()

targetdata = [
      (1, "A"),
      (2, "B"),
      (4, "X"),
      (5, "F")]
mytargetschema = ["id1","name1"]
targetdf = spark.createDataFrame(targetdata,schema=mytargetschema)
targetdf.show()

#--------------------------Through SQL

sourcedf.createOrReplaceTempView("sourcetab")
targetdf.createOrReplaceTempView("targettab")

print("=================Through SQL==========================")
spark.sql("""SELECT COALESCE(s.id, t.id1) AS id,
       CASE
           WHEN s.name IS NULL THEN 'new in target'
           WHEN t.name1 IS NULL THEN 'new in source'
           WHEN s.name != t.name1 THEN 'mismatch'
       END AS comment
FROM sourcetab s
FULL OUTER JOIN targettab t ON s.id = t.id1
WHERE s.name != t.name1 OR s.name IS NULL OR t.name1 IS NULL
""").show()

print("==================Through DSL===============================")
#--------------------------Through DSL
#//Joining two dataframes

joindf = sourcedf.join(targetdf, sourcedf["id"]==targetdf["id1"],"outer")
joindf.show()

#//filtering the columns which are not equal and null

fildf = joindf.filter((col("name") != col("name1")) | col("name").isNull() | col("name1").isNull())
fildf.show()

#//coalesce will replace the null value with next non null value

filnulldf = fildf.withColumn("id",coalesce(col("id"),col("id1"))).drop("id1")
filnulldf.show()

finaldf = filnulldf.withColumn("comment",expr("case when name is null then 'new in target' when name1 is null then 'new in source' when name != name1 then 'mismatch' end")).drop("name","name1")
finaldf.show()


