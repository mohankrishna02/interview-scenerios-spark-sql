// Databricks notebook source
val df1 = Seq((1,"Veg Biryani"),(2,"Veg Fried Rice"),(3,"Kaju Fried Rice"),(4,"Chicken Biryani"),(5,"Chicken Dum Biryani"),(6,"Prawns Biryani"),(7,"Fish Birayani")).toDF("food_id","food_item")
df1.show()

val df2 = Seq((1,5),(2,3),(3,4),(4,4),(5,5),(6,4),(7,4)).toDF("food_id","rating")
df2.show()


// COMMAND ----------

import org.apache.spark.sql.functions._

val joindf = df1.join(df2, df1("food_id") === df2("food_id"), "inner").select(df1("food_id"), df1("food_item"), df2("rating"))
joindf.show()


// COMMAND ----------

val finaldf = joindf.withColumn("stats(out of 5)",expr("repeat('*',rating)"))
finaldf.show()
