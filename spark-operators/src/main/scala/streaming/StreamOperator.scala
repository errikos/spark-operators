package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag

/**
  * Base class for Streaming operators.
  *
  * Defines a simple interface for setting up and returning a Spark streaming context,
  * via the setup function, which takes a SparkStreaming environment (defined in
  * SparkStreaming.scala) and returns a Spark StreamingContext object.
  *
  * @author Ergys Dona <ergys.dona@epfl.ch>
  */
sealed abstract class StreamOperator extends Serializable {

  /**
    * This function shall return a new, ready to be started, Spark StreamingContext object.
    * Only 'lazy' operations shall be performed, i.e. this function shall return immediately.
    */
  def setup(conf: SparkConf, env: Env): StreamingContext
}

object StreamOperator {
  def apply(strategy: String): StreamOperator =
    strategy match {
      case "precise" => PreciseStreamOperator
      case "approx"  => ApproximateStreamOperator
      case _         => throw UnknownStrategyException(s"Unknown strategy: $strategy")
    }
}

object PreciseStreamOperator extends StreamOperator {
  private def printTopK[K, V](msg: String, stream: DStream[(K, V)], k: Int)(
      implicit kt: ClassTag[K],
      vt: ClassTag[V],
      ord: Ordering[(V, K)]): Unit = {
    stream.foreachRDD { rdd =>
      val top = rdd
        .map { _.swap }
        .top(k)
      println(s"$msg: [${top.mkString(",")}]")
    }
  }

  def stateUpdater(vs: Seq[Int], running: Option[Int]): Option[Int] =
    Some(running.getOrElse(0) + vs.sum)

  override def setup(conf: SparkConf, env: Env): StreamingContext = {
    // create a new streaming context object
    val ssc = new StreamingContext(conf, Seconds(env.seconds))
    // create a DStream that represents streaming data from a directory source.
    val linesDStream = ssc.textFileStream(env.inputDirectory)
    // parse the stream and map each line to an IP pair
    val thisBatchIPs = linesDStream.map { x =>
      (x.split("\t")(0), x.split("\t")(1))
    }

    // compute the IP pair hits for the current batch
    val thisBatchHits = thisBatchIPs
      .map { (_, 1) }
      .reduceByKey { _ + _ }
    // find and print the top K hitters for this batch
    printTopK("This batch", thisBatchHits, env.topK)

    // compute the IP pair hits globally
    val globalHits = thisBatchHits
      .updateStateByKey(stateUpdater)
      .checkpoint(Seconds(env.seconds << 3))
    // find and print the top K global hitters
    printTopK("Global", globalHits, env.topK)

    // set checkpoint path and return
    ssc.checkpoint(env.checkpointPath)
    ssc
  }
}

object ApproximateStreamOperator extends StreamOperator {
  override def setup(conf: SparkConf, env: Env): StreamingContext = ???
}
