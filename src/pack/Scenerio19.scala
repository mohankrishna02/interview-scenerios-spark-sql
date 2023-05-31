package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio19 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio19")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("json").option("multiline", "true").load("dbfs:/FileStore/scen.json")
    df.printSchema()
    val finaldf = df.withColumn("multiMedia", explode(col("multiMedia"))).withColumn("dislikes", expr("likeDislike.dislikes")).withColumn("likes", expr("likeDislike.likes")).withColumn("userAction", expr("likeDislike.userAction")).withColumn("createAt", expr("multiMedia.createAt")).withColumn("description", expr("multiMedia.description")).withColumn("id", expr("multiMedia.id")).withColumn("likeCount", expr("multiMedia.likeCount")).withColumn("mediatype", expr("multiMedia.mediatype")).withColumn("name", expr("multiMedia.name")).withColumn("place", expr("multiMedia.place")).withColumn("url", expr("multiMedia.url")).drop("likeDislike", "multiMedia")
    println("flat Schema")
    finaldf.printSchema()
    finaldf.show()
  }
}