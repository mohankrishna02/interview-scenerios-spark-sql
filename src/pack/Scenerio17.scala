package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio17 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio17")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df1 = Seq(
      (1, "Tim", 24, "Kerala", "India"),
      (2, "Asman", 26, "Kerala", "India")).toDF("emp_id", "name", "age", "state", "country")
    df1.show()

    val df2 = Seq(
      (1, "Tim", 24, "Comcity"),
      (2, "Asman", 26, "bimcity")).toDF("emp_id", "name", "age", "address")
    df2.show()

    val findf = df1.join(df2, Seq("emp_id", "name", "age"), "outer")
    findf.show()
  }
}