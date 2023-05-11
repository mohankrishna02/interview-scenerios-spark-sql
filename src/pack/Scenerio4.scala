package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("scenerio-4")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(
      (1, "Mark Ray", "AB"),
      (2, "Peter Smith", "CD"),
      (1, "Mark Ray", "EF"),
      (2, "Peter Smith", "GH"),
      (2, "Peter Smith", "CD"),
      (3, "Kate", "IJ")).toDF("custid", "custname", "address")
    df.show()

    //Through SQL
    df.createOrReplaceTempView("custtab")

    spark.sql("select custid,custname,collect_set(address) as address from custtab group by custid,custname order by custid").show()

    //Through DSL

    val finaldf = df.groupBy("custid", "custname").agg(collect_set("address").as("address")).orderBy("custid").show()

  }
}
