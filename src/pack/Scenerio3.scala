package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("scenerio-3")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val data = Seq(
      (1111, "2021-01-15", 10),
      (1111, "2021-01-16", 15),
      (1111, "2021-01-17", 30),
      (1112, "2021-01-15", 10),
      (1112, "2021-01-15", 20),
      (1112, "2021-01-15", 30)).toDF("sensorid", "timestamp", "values")
    data.show()

    //Through DSL

    val d1 = Window.partitionBy("sensorid").orderBy("values")

    val finaldf = data.withColumn("nextvalues", lead("values", 1) over (d1))
      .filter(col("nextvalues").isNotNull)
      .withColumn("values", expr("nextvalues-values"))
      .drop("nextvalues")
      .show()
  }
}
