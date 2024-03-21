package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Scenerio29 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Scenerio27")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df1 = Seq((1), (2), (3)).toDF("col")
    df1.show()

    val df2 = Seq((1), (2), (3), (4), (5)).toDF("col1")
    df2.show()

    val maxdf = df1.agg(max("col").as("max"))
    maxdf.show()

    val maxsalary = maxdf.select(col("max")).first().getInt(0)

    val joindf = df1.join(df2, df1("col") === df2("col1"), "outer").drop("col")
    joindf.show()

    val finaldf = joindf.filter(col("col1") =!= maxsalary).withColumnRenamed("col1", "col")
    finaldf.show()

  }
}
