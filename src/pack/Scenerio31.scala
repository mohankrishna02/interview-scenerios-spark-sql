package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Scenerio31 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Scenerio27")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")).toDF("col1", "col2", "col3", "col4")
    df.show()

    val contdf = df.withColumn("col", expr("concat(col1,'-',col2,'-',col3,'-',col4,'-')")).drop("col1", "col2", "col3", "col4")
    contdf.show(false)

    val finaldf = contdf.selectExpr("explode(split(col,'-')) as col")
    finaldf.show()

  }
}
