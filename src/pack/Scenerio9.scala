package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio9")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(
      ("a", Seq(1, 1, 1, 3)),
      ("b", Seq(1, 2, 3, 4)),
      ("c", Seq(1, 1, 1, 1, 4)),
      ("d", Seq(3))).toDF("name", "rank")

    df.show()

    val explodedf = df.withColumn("rank", explode(col("rank")))
    explodedf.show()

    val filtdf = explodedf.filter(col("rank") === 1)
    filtdf.show()

    val countdf = filtdf.groupBy("name").agg(count("*").as("count")).orderBy(col("count") desc)
    countdf.show()

    val finaldf = countdf.select(col("name")).first().getString(0)
    println(finaldf)

  }
}