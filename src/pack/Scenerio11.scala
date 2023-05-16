package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio11")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = Seq(
      (1, "Jhon", 4000),
      (2, "Tim David", 12000),
      (3, "Json Bhrendroff", 7000),
      (4, "Jordon", 8000),
      (5, "Green", 14000),
      (6, "Brewis", 6000)).toDF("emp_id", "emp_name", "salary")
    df.show()

    //Through SQL
    df.createOrReplaceTempView("emptab")
    spark.sql("select *,case when salary<5000 then 'C' when salary between 5000 and 10000 then 'B' else 'A' end as grade from emptab ").show()
    
    //Through DSL
    val finaldf = df.withColumn("grade", expr("case when salary<5000 then 'C' when salary between 5000 and 10000 then 'B' else 'A' end")).show()
  }
}