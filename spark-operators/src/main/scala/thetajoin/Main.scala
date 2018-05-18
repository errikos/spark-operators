package thetajoin

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {
    // configure logging
    PropertyConfigurator.configure(getClass.getResourceAsStream("/log4j.properties"))

    val reducers = 10
    val maxInput = 1000
    val inputFile1 = "data/input1_1K.csv"
    val inputFile2 = "data/input2_1K.csv"

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

    val dataset1 = new Dataset(rdd1, schema1)
    val dataset2 = new Dataset(rdd2, schema2)

    val tj = ThetaJoin(dataset1.getRDD.count, dataset2.getRDD.count, reducers, maxInput)
    val res = tj.theta_join(dataset1, dataset2, "num", "num", "=")

    println(res.count)
    //res.saveAsTextFile(output)
  }
}
