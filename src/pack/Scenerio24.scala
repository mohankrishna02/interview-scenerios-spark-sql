package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio24 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio24")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(
      (1, "home"),
      (1, "products"),
      (1, "checkout"),
      (1, "confirmation"),
      (2, "home"),
      (2, "products"),
      (2, "cart"),
      (2, "checkout"),
      (2, "confirmation"),
      (2, "home"),
      (2, "products")).toDF("userid", "page")
    df.show()

    //Through SQL
    df.createOrReplaceTempView("pagetab")
    spark.sql("select userid, collect_list(page) as pages from pagetab group by userid").show()
    
    //Through DSL
    val finaldf = df.groupBy("userid").agg(collect_list("page").as("pages")).show(false)
  }
}