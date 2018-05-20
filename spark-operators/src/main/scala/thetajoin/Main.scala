package thetajoin

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {
    // configure logging
    PropertyConfigurator.configure(getClass.getResourceAsStream("/log4j.properties"))

    val reducers = 16
    val maxInput = 1000
    val inputFile1 = "data/input1_2K.csv"
    val inputFile2 = "data/input2_2K.csv"

    val output = "output"

    val ctx = SparkSession.builder
      .master("local[*]")
      .appName("CS422-Project2")
      .getOrCreate()

    val df1 = ctx.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(inputFile1)

    val df2 = ctx.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(inputFile2)

    val rdd1 = df1.rdd
    val rdd2 = df2.rdd

    val schema1 = df1.schema.toList.map { _.name }
    val schema2 = df2.schema.toList.map { _.name }

    val dataSet1 = new Dataset(rdd1, schema1)
    val dataSet2 = new Dataset(rdd2, schema2)

    val tj = ThetaJoin(dataSet1.getRDD.count, dataSet2.getRDD.count, reducers, maxInput)

    val start = System.nanoTime
    val res = tj.theta_join(dataSet1, dataSet2, "num", "num", "=")
    val end = System.nanoTime

    val seconds = ((end - start) * 1E-9).toInt
    val minutes = seconds / 60

    println(s"Result count: ${res.count}")
    println(s"Time: ${minutes} min, ${seconds} sec")
    //res.saveAsTextFile(output)
  }
}
