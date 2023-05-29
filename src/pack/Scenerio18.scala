package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio18 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio18")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val inputdf = Seq("The Social Dilemma").toDF("word")
    inputdf.show()
    val reverseudf = udf((sentence: String) => sentence.split(" ").map(_.reverse).mkString(" "))
    val outputdf = inputdf.withColumn("reverse word", reverseudf($"word")).drop("word")
    outputdf.show()
  }
}