// Databricks notebook source
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.types._
import spark.implicits._

//creating the dataframe df1
val df1 = Seq((1,"Jhon",Some(17)),(2,"Maria",Some(20)),(3,"Raj",None),(4,"Rachel",Some(18))).toDF("id","name","age")
df1.show()

// COMMAND ----------

//Count null entries in each column
val nullCounts = df1.select(df1.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*)
nullCounts.show()

// COMMAND ----------

//Remove the row with null entires and store them in a new dataframe named df2
val df2 = df1.filter(col("age").isNull)
df2.show()

// COMMAND ----------

//create a new dataframe df3
val df3 = Seq((1,"seatle",82),(2,"london",75),(3,"banglore",60),(4,"boston",90)).toDF("id","city","code")
df3.show()

//join the df1 and df3
val mergedf = df1.join(df3, df1("id") === df3("id"), "inner").select(df1("id"), df1("name"), df1("age"), df3("city"), df3("code"))
mergedf.show()

// COMMAND ----------

//fill the null value with the mean age of students
//calculate the mean age
// Calculate the mean age
val meanage = mergedf.select(mean("age")).first().getDouble(0)

// Fill null values in the 'age' column with the mean age
val filldf = mergedf.na.fill(Map("age" -> meanage))

// Show the resulting DataFrame
filldf.show()

// COMMAND ----------

//Get the students who are 18 years or older
val filterdf = filldf.filter(col("age")>= 18)
filterdf.show()
