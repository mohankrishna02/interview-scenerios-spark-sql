package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio16 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio16")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = Seq(
      (1, "Jhon", "Testing", 5000),
      (2, "Tim", "Development", 6000),
      (3, "Jhon", "Development", 5000),
      (4, "Sky", "Prodcution", 8000)).toDF("id", "name", "dept", "salary")
    df.show()

    val finaldf = df.dropDuplicates("name").orderBy("id")
    finaldf.show()
  }
}