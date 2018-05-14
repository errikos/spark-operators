package cubeoperator

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Main {

  val numReducers: Int = 10
  val inputPath: String = "./data/lineorder_small.tbl"
  val outputPath: String = s"$inputPath.out"

  def main(args: Array[String]) {
    // setup Spark context
    val ctx = SparkSession.builder
      .master("local[16]")
      .appName("CS422-Project2")
      .getOrCreate()

    // load input file into a DataFrame
    val df = ctx.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(inputPath)

    // create Dataset
    val dataset = Dataset(df.rdd, df.schema.map { _.name }.toList)

    // create Cube operator and run
    val cb = CubeOperator(numReducers)
    val groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate")

    val res = cb.cube(dataset, groupingList, "lo_supplycost", "SUM")
    res.saveAsTextFile(outputPath)

    /**
      * The above call corresponds to the query:
      *
      * SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
      * FROM LINEORDER
      * CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
      */
    //Perform the same query using SparkSQL
    val q1 = df
      .cube("lo_suppkey", "lo_shipmode", "lo_orderdate")
      .agg(sum("lo_supplycost") as "sum supplycost")
    q1.show(20)
  }
}
