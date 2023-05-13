package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio10")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(
      (1, 300, "31-Jan-2021"),
      (1, 400, "28-Feb-2021"),
      (1, 200, "31-Mar-2021"),
      (2, 1000, "31-Oct-2021"),
      (2, 900, "31-Dec-2021"))
      .toDF("empid", "commissionamt", "monthlastdate")

    df.show()

    val maxdatedf = df.groupBy(col("empid").as("empid1")).agg(max("monthlastdate").as("maxdate"))
    maxdatedf.show()

    val joindf = df.join(maxdatedf, df("empid") === maxdatedf("empid1") && df("monthlastdate") === maxdatedf("maxdate"), "inner").drop("empid1", "maxdate").show()

  }
}