package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio22 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio22")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = Seq(
      (1, "26-May", 100),
      (1, "27-May", 200),
      (1, "28-May", 300),
      (2, "29-May", 400),
      (3, "30-May", 500),
      (3, "31-May", 600)).toDF("pid", "date", "price")
    df.show()

    //Through SQL
    df.createOrReplaceTempView("ordertab")
    spark.sql("select pid,date,price, sum(price) over(partition by(pid) order by(price)) as new_price from ordertab").show()

    //Through DSL
    val wn = Window.partitionBy("pid").orderBy("price")
    val finaldf = df.withColumn("new_price", sum("price") over (wn)).show()
  }
}