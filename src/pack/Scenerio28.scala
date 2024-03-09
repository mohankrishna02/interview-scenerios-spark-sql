package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Scenerio28 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Scenerio27")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(("A", "AA"), ("B", "BB"), ("C", "CC"), ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")).toDF("child", "parent")
    df.show()

    val joindf = df.as("a").join(df.as("b"), col("a.child") === col("b.parent")).select(
      col("a.child").alias("child_a"),
      col("a.parent").alias("parent_a"),
      col("b.child").alias("child_b"),
      col("b.parent").alias("parent_b")
    )
    joindf.show()

    val findf = joindf.withColumnRenamed("child_a", "parent").withColumnRenamed("parent_a", "grandparent").withColumnRenamed("child_b", "child").drop("parent_b").select("child", "parent", "grandparent")

    findf.show()

    //another way

    val df2 = df.withColumnRenamed("child", "child1").withColumnRenamed("parent", "parent1")
    df2.show()

    val secondjoindf = df.join(df2, df("parent") === df2("child1"), "inner")
    secondjoindf.show()

    val finaldf = secondjoindf.withColumnRenamed("parent1", "grandparent").drop("child1")
    finaldf.show()
  }
}
