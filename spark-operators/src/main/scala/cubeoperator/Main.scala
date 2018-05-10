package cubeoperator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object CubeRunner {

  val numReducers = 10

  def apply(inputPath: String): RDD[(String, Double)] = {
    // setup Spark session and context
    val ctx = SparkSession.builder
      .master("local[16]")
      .appName("CS422-Project2")
      .getOrCreate()
    val sqlContext = ctx.sqlContext

    // load input file into a DataFrame
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(inputPath)

    // create Dataset
    val dataset = Dataset(df.rdd, df.schema map { _.name } toList)

    // create Cube operator and run
    val cb = CubeOperator(numReducers)
    var groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate")
    cb.cube(dataset, groupingList, "lo_supplycost", "SUM")

    /**
      * The above call corresponds to the query:
      * SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
      * FROM LINEORDER
      * CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
      */
  }
}

object Main {
  def main(args: Array[String]) {

    val res = CubeRunner("./data/lineorder_small.tbl")

    // res.saveAsTextFile(outputPath)

    //Perform the same query using SparkSQL
    //    val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
    //      .agg(sum("lo_supplycost") as "sum supplycost")
    //    q1.show

  }
}
