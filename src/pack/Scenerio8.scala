package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio8 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio8")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(
      ("India"),
      ("Pakistan"),
      ("SriLanka")).toDF("teams")

    df.show()

    //Through SQL
    df.createOrReplaceTempView("crickettab")

    //self join query for reference - select a.teams,b.teams from crickettab a inner join crickettab b on a.teams < b.teams

    spark.sql("select concat(a.teams, ' Vs ', b.teams) as matches from crickettab a inner join crickettab b on a.teams < b.teams").show()

    //Through DSL

    val joindf = df.as("a").join(df.as("b"), $"a.teams" < $"b.teams", "inner")
    joindf.show()

    val finaldf = joindf.withColumn("matches", expr("concat(a.teams,' Vs ',b.teams)")).drop("teams", "teams").show()
  }
}