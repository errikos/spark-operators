package thetajoin

import org.apache.spark.rdd.RDD

class ThetaJoin(numR: Long, numS: Long, reducers: Int, bucketSize: Int) extends Serializable {

  /** Sample the boundaries from an RDD.
    *
    * @param rdd the RDD to sample from
    * @param idx the index of the join attribute
    * @return a sorted [[Array]] with the sampled integers
    */
  private def sample(rdd: RDD[Int], idx: Int): Array[Int] = {
    rdd
      .sample(withReplacement = false, fraction = math.sqrt(reducers.toDouble / (numR * numS)))
      .distinct(reducers)
      .collect
      .sorted // collect them as an Array and sort
  }

  private def bucketCount(rdd: RDD[Int], bounds: Array[Int]): Array[Int] = {
    rdd
      .map { v => // for each value, map a (bucket, 1) pair
        (bounds.sliding(2).indexWhere {
          case Array(i, j) => i <= v && v < j
        }, 1)
      }
      .reduceByKey { _ + _ } // reduce by summing the counts per bucket
      .collect
      .sortBy { _._1 } // sort by bucket
      .unzip // split the (bucket, count) pairs to separate Arrays
      ._2 // return the counts Array
  }

  /** Takes as input two data sets and the join condition.
    * Returns the resulting RDD after projecting attr1 and attr2.
    */
  def theta_join(dataSet1: Dataset,
                 dataSet2: Dataset,
                 attr1: String,
                 attr2: String,
                 op: String): RDD[(Int, Int)] = {
    // get the indexes of the join attributes
    val attrR_idx = dataSet1.getSchema.indexOf(attr1) // index of the first (left) join attribute
    val attrS_idx = dataSet2.getSchema.indexOf(attr2) // index of the second (right) join attribute
    // get the data sets as RDDs
    val R = dataSet1.getRDD.map { _.getInt(attrR_idx) } // only keep the join attribute for R
    val S = dataSet2.getRDD.map { _.getInt(attrS_idx) } // only keep the join attribute for S

    // (a) compute the approximate equi-depth histogram for R and S -------------------------------
    val R_bounds = Int.MinValue +: sample(R, attrR_idx) :+ Int.MaxValue // get cr samples from R
    val S_bounds = Int.MinValue +: sample(S, attrS_idx) :+ Int.MaxValue // get cs samples from S
    val R_counts = bucketCount(R, R_bounds) // get the bucket counts for R
    val S_counts = bucketCount(S, S_bounds) // get the bucket counts for S

    println(s"|R| = $numR, |S| = $numS")
    println(s"fraction: ${math.sqrt(reducers.toDouble / (numR * numS))}")
    println(s"R bounds: ${R_bounds.mkString(",")}")
    println(s"R counts: ${R_counts.mkString(",")} (=${R_counts.sum})")
    println(s"S bounds: ${S_bounds.mkString(",")}")
    println(s"S counts: ${S_counts.mkString(",")} (=${S_counts.sum})")

    ???
  }

  /** this method takes as input two lists of values that belong to the same partition
    * and performs the theta join on them. Both data sets are lists of tuples (Int, Int)
    * where ._1 is the partition number and ._2 is the value.
    */
  private def |><|(dat1: Iterator[(Int, Int)],
                   dat2: Iterator[(Int, Int)],
                   op: String): Iterator[(Int, Int)] = {
    var res = List.empty[(Int, Int)]
    val dat2List = dat2.toList

    while (dat1.hasNext) {
      val row1 = dat1.next
      for (row2 <- dat2List) {
        if (ThetaJoin.checkCondition(row1._2, row2._2, op)) {
          res = res :+ (row1._2, row2._2)
        }
      }
    }
    res.iterator
  }
}

object ThetaJoin {
  def apply(numR: Long, numS: Long, reducers: Int, bucketSize: Int): ThetaJoin =
    new ThetaJoin(numR, numS, reducers, bucketSize)

  def checkCondition(value1: Int, value2: Int, op: String): Boolean = {
    op match {
      case "="  => value1 == value2
      case "<"  => value1 < value2
      case "<=" => value1 <= value2
      case ">"  => value1 > value2
      case ">=" => value1 >= value2
    }
  }
}
