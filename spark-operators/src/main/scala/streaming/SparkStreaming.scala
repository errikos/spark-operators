package streaming

import org.apache.spark._
import org.apache.spark.streaming._

class Env(args: Array[String]) extends Serializable {
  val checkpointPath = "/tmp/task3_checkpoint" // the checkpoint base path
  val inputDirectory = args(0) // the directory in which the stream is expected
  val seconds: Int = args(1).toInt // seconds per window
  val topK: Int = args(2).toInt // track the k first hitters; only relevant for "precise" strategy
  val strategy: String = args(3).toLowerCase // "precise" or "approx"

  // arguments relative to "approx" strategy only
  val eps: Option[Double] = args.lift(4).map { _.toDouble }
  val confidence: Option[Double] = args.lift(5).map { _.toDouble }
  val observedPair: Option[(String, String)] = args.lift(6).map { p =>
    (p.split(",")(0), p.split(",")(1))
  }
}

object Env {
  def apply(args: Array[String]) = new Env(args)
}

/**
  * This class acts as the streaming initiator, as well as the environment
  * to the StreamOperator class hierarchy.
  *
  * Streaming operation is triggered with a call to consume().
  */
class SparkStreaming(val sparkConf: SparkConf, val args: Array[String]) extends Serializable {

  val env = Env(args)

  // create a StreamingContext, the main entry point for all streaming functionality
  private def createContext: StreamingContext = {
    // get the appropriate streaming operator (calls StreamOperator.apply)
    val streamOperator = StreamOperator(env)
    // setup the streaming operations and return the context
    streamOperator.setup(sparkConf, env)
  }

  def consume() {
    // get or create a new streaming context
    val ssc = StreamingContext.getOrCreate(env.checkpointPath, createContext _)
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
