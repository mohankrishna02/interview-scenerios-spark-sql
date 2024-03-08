package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Scenerio27 {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Scenerio27")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq((1,60000,2018),(1,70000,2019),(1,80000,2020),(2,60000,2018),(2,65000,2019),(2,65000,2020),(3,60000,2018),(3,65000,2019)).toDF("empid","salary","year")
    df.show()

    val wn = Window.partitionBy("empid").orderBy(col("year"))

    val lagdf = df.withColumn("diff",lag("salary",1) over(wn))
    lagdf.show()

    val finaldf = lagdf.withColumn("incresalary",expr("salary - diff")).drop("diff").na.fill(0).orderBy("empid","year")
    finaldf.show()


  }
}
