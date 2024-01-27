package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio26 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio25")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val sourcedf = Seq(
      (1, "A"),
      (2, "B"),
      (3, "C"),
      (4, "D")).toDF("id", "name")
    sourcedf.show()

    val targetdf = Seq(
      (1, "A"),
      (2, "B"),
      (4, "X"),
      (5, "F")).toDF("id1", "name1")
    targetdf.show()

    sourcedf.createOrReplaceTempView("sourcetab")
    targetdf.createOrReplaceTempView("targettab")

    spark.sql("""SELECT COALESCE(s.id, t.id1) AS id,
       CASE
           WHEN s.name IS NULL THEN 'new in target'
           WHEN t.name1 IS NULL THEN 'new in source'
           WHEN s.name != t.name1 THEN 'mismatch'
       END AS comment
FROM sourcetab s
FULL OUTER JOIN targettab t ON s.id = t.id1
WHERE s.name != t.name1 OR s.name IS NULL OR t.name1 IS NULL
""").show()

    //Joining two dataframes

    val joindf = sourcedf.join(targetdf, col("id") === col("id1"), "outer")
    joindf.show()

    //filtering the columns which are not equal and null

    val filterdf = joindf.filter(col("name") =!= col("name1") || col("name").isNull || col("name1").isNull)
    filterdf.show()

    //coalesce will replace the null value with next non null value

    val nullfildf = filterdf.withColumn("id", coalesce(col("id"), col("id1"))).drop("id1")
    nullfildf.show()

    val finaldf = nullfildf.withColumn("comment", expr("case when name is null then 'new in target' when name1 is null then 'new in source' when name!=name1 then 'mismatch' end")).drop("name", "name1")
    finaldf.show()

  }
}
