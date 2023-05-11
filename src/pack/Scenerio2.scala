package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("scenerio-2")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(
      (1, "1-Jan", "Ordered"),
      (1, "2-Jan", "dispatched"),
      (1, "3-Jan", "dispatched"),
      (1, "4-Jan", "Shipped"),
      (1, "5-Jan", "Shipped"),
      (1, "6-Jan", "Delivered"),
      (2, "1-Jan", "Ordered"),
      (2, "2-Jan", "dispatched"),
      (2, "3-Jan", "shipped")).toDF("orderid", "statusdate", "status")

    df.show()

    //Through SQL
    df.createOrReplaceTempView("ordertab")
    spark.sql("select * from ordertab where status = 'dispatched' and orderid in(select orderid from ordertab where status = 'Ordered')").show()

    //Through DSL
    val result = df.filter(
      col("status") === "dispatched" &&
        col("orderid").isin(
          df.filter(col("status") === "Ordered").select("orderid").map(_.getInt(0)).collect(): _*))
    result.show()

  }
}
