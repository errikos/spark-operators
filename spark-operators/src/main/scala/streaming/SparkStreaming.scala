package streaming

import org.apache.spark._
import org.apache.spark.streaming._

class SparkStreaming(val sparkConf: SparkConf, val args: Array[String]) {

  val inputDirectory = args(0) // the directory in which the stream is expected
  val seconds: Int = args(1).toInt // seconds per window
  val topK: Int = args(2).toInt // number of heavy hitters stored
  val strategy: String = args(3).toLowerCase // "precise" or "approx"

  // create a StreamingContext, the main entry point for all streaming functionality
  val ssc = new StreamingContext(sparkConf, Seconds(seconds))

  def consume() {
    // create a DStream that represents streaming data from a directory source.
    val linesDStream = ssc.textFileStream(inputDirectory)

    // parse the stream. (line -> (IP1, IP2))
    val words = linesDStream.map { x =>
      (x.split("\t")(0), x.split("\t")(1))
    }

    // get the appropriate streaming operator (calls StreamOperator.apply)
    val streamOperator = StreamOperator(strategy, words)
    // setup the streaming operations
    streamOperator.setup(this)

    // start the computation
    ssc.start()

    // wait for the computation to terminate
    ssc.awaitTermination()
  }
}

object SparkStreaming {
  def apply(sparkConf: SparkConf, args: Array[String]): SparkStreaming =
    new SparkStreaming(sparkConf, args)
}
