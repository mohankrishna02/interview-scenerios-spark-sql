package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio21 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio21")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(
      ("SEA", "SF", 300),
      ("CHI", "SEA", 2000),
      ("SF", "SEA", 300),
      ("SEA", "CHI", 2000),
      ("SEA", "LND", 500),
      ("LND", "SEA", 500),
      ("LND", "CHI", 1000),
      ("CHI", "NDL", 180)).toDF("from", "to", "dist")
    df.show()
    //Through SQL
    df.createOrReplaceTempView("trip")
    spark.sql("""SELECT r1.from, r1.to, (r1.dist + r2.dist) AS roundtrip_dist
FROM trip r1
JOIN trip r2 ON r1.from = r2.to AND r1.to = r2.from
WHERE r1.from < r1.to
""").show()

    //Through DSL
    val finaldf = df.as("r1").join(
      df.as("r2"),
      (col("r1.from") === col("r2.to")) && (col("r1.to") === col("r2.from"))).where(
        col("r1.from") < col("r1.to")).select(col("r1.from"), col("r1.to"),
          (col("r1.dist") + col("r2.dist")).alias("roundtrip_dist"))
    finaldf.show()
  }
}
