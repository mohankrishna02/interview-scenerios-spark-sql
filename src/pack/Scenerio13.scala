package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio13 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio13")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(
      (1, "Jhon", "Development"),
      (2, "Tim", "Development"),
      (3, "David", "Testing"),
      (4, "Sam", "Testing"),
      (5, "Green", "Testing"),
      (6, "Miller", "Production"),
      (7, "Brevis", "Production"),
      (8, "Warner", "Production"),
      (9, "Salt", "Production")).toDF("emp_id", "emp_name", "dept")
    df.show()

    //Through SQL
    df.createOrReplaceTempView("emptab")
    spark.sql("SELECT dept, COUNT(*) AS total FROM emptab GROUP BY dept").show()

    //Through DSL
    val finaldf = df.groupBy(col("dept")).agg(count("*").as("total")).show()
  }
}