package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object Scenerio12 {

  //creating UDF functions for masked data, here email(0) is it will take first letter i.e 0th index and email.substring(8) is it will take the string from 8th index position to end of the string
  def maskEmail(email: String): String = {
    email(0) + "**********" + email.substring(8)
  }

  //creating UDF functions for masked data, here mobile.substring(0, 2) is it will take string from Index 0 to 2 letters and mobile.substring(mobile.length - 3)calculates the starting index for the substring. It subtracts 3 from the length of the mobile string to determine the appropriate index to start the substring.

  def maskMobile(mobile: String): String = {
    mobile.substring(0, 2) + "*****" + mobile.substring(mobile.length - 3)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scenerio9")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val maskEmailUDF = udf[String, String](maskEmail)
    val maskMobileUDF = udf[String, String](maskMobile)

    val df = Seq(("Renuka1992@gmail.com", "9856765434"), ("anbu.arasu@gmail.com", "9844567788")).toDF("email", "mobile")
    df.show()

    val maskedDF = df.withColumn("email", maskEmailUDF(col("email")))
      .withColumn("mobile", maskMobileUDF(col("mobile")))
    maskedDF.show()
  }
}