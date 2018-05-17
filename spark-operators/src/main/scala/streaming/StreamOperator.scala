package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag
import scala.util.Random
import scala.util.hashing.MurmurHash3._

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
  def apply(env: Env): StreamOperator =
    env.strategy match {
      case "precise" =>
        PreciseCountOperator()
      case "approx" =>
        // try to get relative error and confidence arguments
        val eps = env.eps.getOrElse(throw MissingArgumentException("eps (relative error)"))
        val d = env.confidence.getOrElse(throw MissingArgumentException("d (confidence)"))
        // compute min-count sketch depth and width
        val width = math.ceil(2 / eps).toInt
        val depth = math.ceil(-math.log(1 - d) / math.log(2)).toInt
        println(s"Configuring with width=$width, depth=$depth")
        MinCountSketchOperator(depth, width, env)
      case other => throw UnknownStrategyException(s"Unknown strategy: $other")
    }
}

case class PreciseCountOperator() extends StreamOperator {
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

  private def stateUpdater(vs: Seq[Int], running: Option[Int]): Option[Int] =
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

case class MinCountSketchOperator(depth: Int, width: Int, env: Env) extends StreamOperator {

  /**
    * Initialize `depth` seeds, one for each sketch row. The hash functions will be
    * applied seeded with the respective seed. Models the pairwise independent hash functions.
    */
  private val seeds = (0 until depth).map { _ =>
    Random.nextInt
  }
  private val uSeed = stringHash("CS422-P2-T3") // used for the initial Murmur3 algorithm seeding
  private val observedPair = env.observedPair.getOrElse(throw MissingArgumentException("IP pair"))

  private def stateUpdater(vs: Seq[Int], running: Option[Int]): Option[Int] =
    Some(running.getOrElse(0) + vs.sum)

  // calculates the hash function value for `item` seeded with `seed`
  private def ###[T](item: T, seed: Int)(implicit ct: ClassTag[T]): Int =
    // reset the MSB of the result (convert to positive Int) and restrict to width range
    (finalizeHash(mixLast(mix(uSeed, seed), item.##), 0) & (~0 >>> 1)) % width

  private def printEstimation(msg: String, stream: DStream[((Int, Int), Int)]): Unit = {
    stream
      .filter {
        // filter the RDD and keep only the columns that correspond to the observed IP pair
        case ((row, col), _) =>
          col == ###(observedPair, seeds(row))
      }
      .map {
        // replace the (cell, value) pairs with (value, 1) pairs; this is to compute the
        // min(value) and the count of values found for the observed pair in the local sketch
        case (_, value) =>
          (value, 1)
      }
      .reduce {
        // reduce to compute min(value) of cells and count of reduced values
        // take the computed value into account if and only if count == depth
        case ((v1, s1), (v2, s2)) =>
          (math.min(v1, v2), s1 + s2)
      }
      .foreachRDD { rdd =>
        // if there is at least one element in the RDD and the count of reduced values == depth,
        // then report the estimated count for this batch, otherwise report zero
        val estimation = rdd.take(1) match {
          case Array((est, count)) if count == depth => est
          case _                                     => 0
        }
        println(s"$msg: [($estimation,$observedPair)]")
      }
  }

  /**
    * Sets up the Min-Count Sketch operator. We implement a distributed sketch.
    *
    * What this means is that our state consists of (w x d) integers,
    * each one corresponding to one cell of the sketch array.
    *
    * Each time a new batch arrives, we take the keys (IP pairs) and we map them to d
    * (row, column) pairs, with an Int key (=1). Then, we effectively calculate the
    * "local" state of each cell by reducing by key and summing. From this we can extract
    * the estimation for the observed IP pair for this batch.
    *
    * Once this is done, we can update the saved states by key (=IP pairs)
    * and print the estimation based on the global states.
    */
  override def setup(conf: SparkConf, env: Env): StreamingContext = {
    // create a new streaming context object
    val ssc = new StreamingContext(conf, Seconds(env.seconds))
    // create a DStream that represents streaming data from a directory source.
    val linesDStream = ssc.textFileStream(env.inputDirectory)
    // parse the stream and map each line to an IP pair
    val thisBatchIPs = linesDStream.map { x =>
      (x.split("\t")(0), x.split("\t")(1))
    }

    // calculate the min-count sketch state for the current batch
    val thisBatchSketch = thisBatchIPs
      .flatMap { key =>
        // for each IP pair, yield the (row, col) sketch pairs with a value of 1
        seeds.zipWithIndex.map {
          case (seed, idx) =>
            ((idx, ###(key, seed)), 1)
        }
      }
      .reduceByKey { _ + _ } // calculate the cumulative cell sums for the local state

    // calculate and print the estimation for the current batch (local estimation)
    printEstimation("This batch", thisBatchSketch)

    // calculate and print the estimation for all batches (global estimation)
    val globalSketch = thisBatchSketch
      .updateStateByKey(stateUpdater)
      .checkpoint(Seconds(env.seconds << 3))
    printEstimation("Global", globalSketch)

    // set checkpoint path and return
    ssc.checkpoint(env.checkpointPath)
    ssc
  }

}
