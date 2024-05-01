// Databricks notebook source
val familydf = Seq(("c00dac11bde74750b4d207b9c182a85f", "Alex Thomas", 9),("eb6f2d3426694667ae3e79d6274114a4", "Chris Gray", 2),("3f7b5b8e835d4e1c8b3e12e964a741f3", "Emily Johnson", 4),("9a345b079d9f4d3cafb2d4c11d20f8ce", "Michael Brown", 6),("e0a5f57516024de2a231d09de2cbe9d1", "Jessica Wilson", 3)).toDF("id","name","family_size")
familydf.show()

val countrydf = Seq(("023fd23615bd4ff4b2ae0a13ed7efec9", "Bolivia", 2 , 4),("be247f73de0f4b2d810367cb26941fb9", "Cook Islands", 4,8),("3e85ab80a6f84ef3b9068b21dbcc54b3", "Brazil", 4,7),("e571e164152c4f7c8413e2734f67b146", "Australia", 5,9),("f35a7bb7d44342f7a8a42a53115294a8", "Canada", 3,5),("a1b5a4b5fc5f46f891d9040566a78f27", "Japan", 10,12)).toDF("id","name","min_size","max_size")
countrydf.show()

// COMMAND ----------

import org.apache.spark.sql.functions._

val joindf = familydf.join(countrydf, familydf("family_size") >= countrydf("min_size") && familydf("family_size") <= countrydf("max_size"),"inner").select(familydf("name"), familydf("family_size"), countrydf("name").as("country_name"),  countrydf("min_size"), countrydf("max_size"))
joindf.show()


// COMMAND ----------

val groupdf = joindf.groupBy(familydf("name")).agg(count("*").alias("number_of_countries"))
groupdf.show()

// COMMAND ----------

val finaldf = groupdf.agg(expr("max(number_of_countries)").alias("number_of_countries"))
finaldf.show()

// COMMAND ----------

import org.apache.spark.sql.expressions._

//another way 
val wn = Window.orderBy(desc("number_of_countries"))

val rankdf = groupdf.withColumn("rank",row_number() over(wn))
rankdf.show()

val finaldf2 = rankdf.filter(col("rank")===1).drop("rank")
finaldf2.show()
