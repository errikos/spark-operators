package streaming

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.hashing.MurmurHash3._

class CountMinSketch(val depth: Int, val width: Int) extends Serializable {

  /**
    * Initialize `depth` seeds, one for each sketch row. The hash functions will be
    * applied seeded with the respective seed. Models the pairwise independent hash functions.
    * `uSeed` is used for the initial Murmur3 algorithm seeding.
    */
  private val seeds = (0 until depth).map { _ => Random.nextInt }
  private val uSeed = stringHash("CS422-P2-T3")

  // the sketch (width x depth) array
  private var sketch = seeds.map { _ =>
    mutable.ArraySeq.fill(width)(0)
  }

  // calculates the hash function value for `item` seeded with `seed`
  private def h[T](item: T, seed: Int)(implicit ct: ClassTag[T]): Int =
    // reset the MSB of the result (convert to positive Int) and restrict to width range
    (finalizeHash(mixLast(mix(uSeed, seed), item.##), 0) & (~0 >>> 1)) % width

  /**
    * Updates the sketch for `item` by adding `x` to each respective counter.
    * `item` may be of any type T and it is hashed before it is fed to the hash function.
    */
  def update[T](item: T, x: Int = 1)(implicit ct: ClassTag[T]): Unit =
    seeds.zipWithIndex.foreach {
      case (s, idx) => // for each (seed, sketch row) pair
        sketch(idx)(h(item, s)) += x
    }

  /**
    * Estimate and return the count of `item` in the sketch.
    * `item` may be of any type T and it is hashed before it is fed to the hash function.
    * Of course, it only makes sense to estimate counts for already counted items.
    */
  def estimate[T](item: T)(implicit ct: ClassTag[T]): Int =
    seeds.zipWithIndex.map {
      case (s, idx) =>
        val col = math.abs(finalizeHash(mixLast(mix(uSeed, s), item.##), 0)) % width
        sketch(idx)(col)
    }.min

  def mergeInPlace(other: CountMinSketch): CountMinSketch = {
    sketch = sketch.zip(other.sketch).map { case (r1, r2) =>
      r1.zipWithIndex.map { case (x, idx) => x + r2(idx) }
    }
    this
  }

  def printSketch(): Unit = {
    sketch.foreach { println }
  }
}

object CountMinSketch {
  def apply(eps: Double, confidence: Double): CountMinSketch = {
    val depth = math.ceil(2 / eps).toInt
    val width = math.ceil(-math.log(1 - confidence) / math.log(2)).toInt
    // Above calculations taken from:
    // https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/util/sketch/CountMinSketch.html
    new CountMinSketch(depth, width)
  }
}
