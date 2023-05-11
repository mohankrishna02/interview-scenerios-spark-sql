package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio6")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark.createDataFrame(Seq(
      ("1", "a", "10000"),
      ("2", "b", "5000"),
      ("3", "c", "15000"),
      ("4", "d", "25000"),
      ("5", "e", "50000"),
      ("6", "f", "7000")))
      .toDF("empid", "name", "salary")
    df.show()
    
    //Through SQL
    df.createOrReplaceTempView("emptab")
    spark.sql("select *, case when salary > 10000 then 'Manager' else 'Employee' end as Designation from emptab").show()
    
    //Through DSL
    val finaldf = df.withColumn("Desgination", expr("case when salary > 10000 then 'Manager' else 'Employee' end"))
    finaldf.show()
  }
}