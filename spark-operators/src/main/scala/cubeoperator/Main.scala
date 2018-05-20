package cubeoperator

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Main {
  PropertyConfigurator.configure(getClass.getResourceAsStream("/log4j.properties"))

  val numReducers: Int = 24
  val inputPath: String = "./data/lineorder_big.tbl"
  val outputPath: String = s"$inputPath.out"

  def main(args: Array[String]) {
    // setup Spark context
    val ctx = SparkSession.builder
      .master("local[*]")
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

    val start = System.nanoTime
    // choose between "optimized" and naive
//    val res = cb.cube(dataset, groupingList, "lo_supplycost", "SUM")
    val res = cb.cube_naive(dataset, groupingList, "lo_supplycost", "SUM")
    val count = res.count // get count (causes the operations to be performed)

    val end = System.nanoTime

    val sec = (end - start) / Math.pow(10, 9)
    println(s"Time: $sec sec")

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

    // is everything OK?
    assert(q1.count == count)
  }
}
