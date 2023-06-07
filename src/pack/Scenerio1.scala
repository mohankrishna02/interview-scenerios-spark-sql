package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object Scenerio1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Scenerio1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = Seq(
      ("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),
      ("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),
      ("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),
      ("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
      ("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin"))
      .toDF("workerid", "firstname", "lastname", "salary", "joiningdate", "depart")

    df.show()
    //Through SQL Query
    df.createOrReplaceTempView("worktab")

    spark.sql("select a.workerid,a.firstname,a.lastname,a.salary,a.joiningdate,a.depart from worktab a, worktab b where a.salary=b.salary and a.workerid !=b.workerid").show()
    //Through Spark DSL
    val finaldf = df.as("a").join(df.as("b"), $"a.salary" === $"b.salary" && $"a.workerid" =!= $"b.workerid").select($"a.workerid", $"a.firstname", $"a.lastname", $"a.salary", $"a.joiningdate", $"a.depart").show()
    
    //Another way 
    val finaldf = df.as("a").join(df.as("b")).where(col("a.salary")===col("b.salary") && col("a.workerid") =!= col("b.workerid")).select($"a.workerid",$"a.firstname",$"a.lastname",$"a.salary",$"a.joiningdate",$"a.depart").show()
  }
}
