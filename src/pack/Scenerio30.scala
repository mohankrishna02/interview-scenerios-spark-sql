package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Scenerio30 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Scenerio27")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df1 = Seq((1, "A", "A", 1000000), (2, "B", "A", 2500000), (3, "C", "G", 500000), (4, "D", "G", 800000), (5, "E", "W", 9000000), (6, "F", "W", 2000000)).toDF("emp_id", "name", "dept_id", "salary")
    df1.show()

    val df2 = Seq(("A", "AZURE"), ("G", "GCP"), ("W", "AWS")).toDF("dept_id1", "dept_name")
    df2.show()

    val joindf = df1.join(df2, df1("dept_id") === df2("dept_id1"), "inner").drop("dept_id1")
    joindf.show()

    val wn = Window.partitionBy("dept_id").orderBy(col("salary").desc)

    val rankdf = joindf.withColumn("rank", dense_rank() over (wn))
    rankdf.show()

    val finaldf = rankdf.filter(col("rank") === 2).drop("rank").select("emp_id", "name", "dept_name", "salary")
    finaldf.show()
  }
}
