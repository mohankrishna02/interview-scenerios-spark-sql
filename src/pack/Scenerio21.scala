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
    spark.sql("select a.from,a.to,(a.dist+b.dist) as total_dist from trip a join trip b on a.from=b.to and a.to=b.from").show()

    //Through DSL
    val finaldf = df.as("a").join(df.as("b"), col("a.from") === col("b.to") && col("a.to") === col("b.from")).select(col("a.from"), col("a.to"), (col("a.dist") + col("b.dist")).as("total_dist"))
    finaldf.show()
  }
}