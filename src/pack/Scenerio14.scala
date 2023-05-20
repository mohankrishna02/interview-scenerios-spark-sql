package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio14 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio14")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq((203040, "rajesh", 10, 20, 30, 40, 50)).toDF("rollno", "name", "telugu", "english", "maths", "science", "social")
    df.show()

    //Through SQL
    df.createOrReplaceTempView("marks")
    spark.sql("select *, (telugu+english+maths+science+social) as total from marks").show()

    //Through DSL
    val finaldf = df.withColumn("total", expr("telugu+english+maths+science+social")).show()
  }
}