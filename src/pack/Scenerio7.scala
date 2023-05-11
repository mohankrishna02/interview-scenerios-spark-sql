package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio6")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.createDataFrame(Seq(
      (1, 100, 2010, 25, 5000),
      (2, 100, 2011, 16, 5000),
      (3, 100, 2012, 8, 5000),
      (4, 200, 2010, 10, 9000),
      (5, 200, 2011, 15, 9000),
      (6, 200, 2012, 20, 7000),
      (7, 300, 2010, 20, 7000),
      (8, 300, 2011, 18, 7000),
      (9, 300, 2012, 20, 7000)))
      .toDF("sale_id", "product_id", "year", "quantity", "price")
    df.show()

    //Through SQL
    df.createOrReplaceTempView("salestab")
    spark.sql("SELECT *FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY year ORDER BY quantity DESC) AS rank FROM salestab) AS rankdf WHERE rank = 1 ORDER BY sale_id").show()

    //Through DSL
    val win = Window.partitionBy("year").orderBy(col("quantity").desc)

    val rankdf = df.withColumn("rank", dense_rank() over (win))
    rankdf.show()

    val finaldf = rankdf.filter(col("rank") === "1").drop("rank").orderBy("sale_id").show()

  }
}