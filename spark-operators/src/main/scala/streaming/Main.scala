package streaming

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf

object Main {
  def main(args: Array[String]) {
    // configure logging
    PropertyConfigurator.configure(getClass.getResourceAsStream("/log4j.properties"))
    // create configuration and start consuming
    val sparkConf = new SparkConf().setAppName("CS422-Project2-Task3") //.setMaster("local[16]")
    val streaming = SparkStreaming(sparkConf, args)
    streaming.consume()
  }
}
