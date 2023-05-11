package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("scenerio-5")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df1 = Seq(
      (1, "abc", 31, "abc@gmail.com"),
      (2, "def", 23, "defyahoo.com"),
      (3, "xyz", 26, "xyz@gmail.com"),
      (4, "qwe", 34, "qwegmail.com"),
      (5, "iop", 24, "iop@gmail.com"))
      .toDF("id", "name", "age", "email")
    df1.show()
    
    val df2 = Seq(
      (11, "jkl", 22, "abc@gmail.com", 1000),
      (12, "vbn", 33, "vbn@yahoo.com", 3000),
      (13, "wer", 27, "wer", 2000),
      (14, "zxc", 30, "zxc.com", 2000),
      (15, "lkj", 29, "lkj@outlook.com", 2000))
      .toDF("id", "name", "age", "email", "salary")
    df2.show()
    
    //number of partiion in df
    val partcount = df1.rdd.getNumPartitions
    println("Number of partition:- " + partcount)

    val df3 = df1.withColumn("salary", lit(1000))
    df3.show()

    //append df2 and df3, and form df4
    val df4 = df2.union(df3).orderBy(col("id") asc)
    df4.show()

    //Remove records which have invalid email from df4, emails with @ are considered to be valid.
    val rmdf = df4.filter(col("email").rlike("@"))
    rmdf.show()
    
    //Write df4 to a target location, by partitioning on salary.
    rmdf.write.format("parquet").partitionBy("salary").save("D:/BigData/Processed Datasets/interdata")


  }
}
