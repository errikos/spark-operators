package streaming

import org.apache.spark.SparkConf

object Main {
  def main(args: Array[String]) {
    val output = "output"
    val sparkConf = new SparkConf().setAppName("CS422-Project2-Task3") //.setMaster("local[16]")

    val streaming = new SparkStreaming(sparkConf, args)

    streaming.consume()
  }
}
