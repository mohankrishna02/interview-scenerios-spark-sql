package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio20 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio20")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("json").option("multiline", "true").load("dbfs:/FileStore/flatjson/part-00000-tid-3675309499584050336-b8650962-dec3-4fe4-a204-c914090f019e-21-1-c000.json")
    df.printSchema()
    val compdf = df.select(
      col("code"),
      col("commentCount"),
      col("createdAt"),
      col("description"),
      col("feedsComment"),
      col("id"),
      col("imagePaths"),
      col("images"),
      col("isdeleted"),
      col("lat"),
      struct(col("dislikes"), col("likes"), col("userAction")).as("likeDislike"),
      col("lng"),
      col("location"),
      col("mediatype"),
      col("msg"),
      array(
        struct(
          col("createAt"),
          col("description"),
          col("id"), col("likeCount"),
          col("mediatype"),
          col("name"),
          col("place"),
          col("url")).as("element")).as("multiMedia"),
      col("name"),
      col("profilePicture"),
      col("title"),
      col("userId"),
      col("videoUrl"),
      col("totalFeed"))
    compdf.printSchema()
  }
}