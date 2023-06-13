package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio23 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio23")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq((1, 5), (2, 6), (3, 5), (3, 6), (1, 6)).toDF("customer_id", "product_key")
    df.show()
    val df2 = Seq((5), (6)).toDF("product_key")
    df2.show()
    val finaldf = df.join(df2, Seq("product_key"), "inner").drop("product_key").distinct().filter(col("customer_id") =!= 2)
    finaldf.show()
  }
}