// Databricks notebook source
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.types._
import spark.implicits._

val data = Seq(("2020-05-30","Headphone"),("2020-06-01","Pencil"),("2020-06-02","Mask"),("2020-05-30","Basketball"),("2020-06-01","Book"),("2020-06-02","Mask"),("2020-05-30","T-Shirt")).toDF("sell_date","product")
data.show()

// COMMAND ----------

val transdf = data.groupBy("sell_date").agg(collect_set("product").alias("products"),size(collect_set("product")).alias("num_sell"))
transdf.show()
