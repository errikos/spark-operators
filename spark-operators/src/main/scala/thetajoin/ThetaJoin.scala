package thetajoin

import org.apache.spark.rdd.RDD

class ThetaJoin(numR: Long, numS: Long, reducers: Int, bucketSize: Int) extends Serializable {

  /** Sample the boundaries from an RDD.
    *
    * @param rdd the RDD to sample from
    * @param num the size of the sample to take. Note that the number of elements returned
    *            may be smaller than `num`, since any duplicates will be dropped.
    * @return a sorted [[Array]] with the sampled integers
    */
  private def sample(rdd: RDD[Int], num: Int): Array[Int] = {
    rdd
    // sample twice as requested elements, to reduce the probability
    // that the distinct elements are fewer than requested
      .takeSample(withReplacement = false, num = 2 * num)
      .distinct // keep only distinct values from sample
      .take(num) // now take as many as requested
      .sorted // and sort
  }

  /** Given an RDD of Integers, counts the values per bucket.
    * Bucket boundaries are determined by the `bounds` parameter.
    *
    * @param rdd the RDD containing the values to count
    * @param bounds the bucket boundaries
    * @return the counts per bucket as an [[Array]] whose position `i` represents bucket `i`
    */
  private def bucketCount(rdd: RDD[Int], bounds: Seq[Int]): IndexedSeq[Long] = {
    val paired = rdd
      .map { v => // for each value, map a (bucket#, 1) pair
        (bounds.sliding(2).indexWhere { case Seq(_, to) => v <= to }, 1l)
      }
      .reduceByKey { _ + _ } // reduce by summing the counts per bucket
      .collect
      .sortBy { _._1 } // sort by bucket
    for (i <- 1 until bounds.length) // for each bucket
      yield // did it have any elements?
        paired.indexWhere { case (bucket, _) => i - 1 == bucket } match {
          case -1 => 0l // if not, insert a zero for its count
          case ii => paired(ii)._2 // otherwise, insert the count found
        }
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
    val R_bounds = sample(R, math.sqrt((numS * reducers) / numR).round.toInt) // get samples from R
    val S_bounds = sample(S, math.sqrt((numR * reducers) / numS).round.toInt) // get samples from S
    val R_boundsEncl = (R_bounds.toSet ++ Set(Int.MinValue, Int.MaxValue)).toSeq.sorted
    val S_boundsEncl = (S_bounds.toSet ++ Set(Int.MinValue, Int.MaxValue)).toSeq.sorted
    val R_counts = bucketCount(R, R_boundsEncl) // get the bucket counts for R
    val S_counts = bucketCount(S, S_boundsEncl) // get the bucket counts for S

    // some debug messages
    println(s"|R| = $numR, |S| = $numS")
    println(s"fraction: ${math.sqrt(reducers.toDouble / (numR * numS))}")
    println(s"R bounds: ${R_boundsEncl.mkString(",")}")
    println(s"R counts: ${R_counts.mkString(",")} (=${R_counts.sum})")
    println(s"S bounds: ${S_boundsEncl.mkString(",")}")
    println(s"S counts: ${S_counts.mkString(",")} (=${S_counts.sum})")

    // (b) calculate the buckets to be assigned to the reducers (M-Bucket-I algorithm) ------------
//    val opFun = ThetaJoin.getOp(op) // get the operator function
    val mbi = MBucketInput(numR, R_bounds, R_counts, numS, S_bounds, S_counts, bucketSize, op)
//    val assign = mbi.coverMatrix // create an M-Bucket-I object and calculate the assignment

    ???
  }

  /** this method takes as input two lists of values that belong to the same partition
    * and performs the theta join on them. Both data sets are lists of tuples (Int, Int)
    * where ._1 is the partition number and ._2 is the value.
    */
  private def |><|(dat1: Iterator[(Int, Int)],
                   dat2: Iterator[(Int, Int)],
                   op: (Int, Int) => Boolean): Iterator[(Int, Int)] = {
    val res = scala.collection.mutable.MutableList.empty[(Int, Int)]
    val dat2List = dat2.toSeq

    for (row1 <- dat1)
      for (row2 <- dat2List)
        if (op(row1._2, row2._2))
          res += ((row1._2, row2._2))
    res.iterator
  }
}

object ThetaJoin {
  def apply(numR: Long, numS: Long, reducers: Int, bucketSize: Int): ThetaJoin =
    new ThetaJoin(numR, numS, reducers, bucketSize)

  def getOp(op: String): (Int, Int) => Boolean = op match {
    case "<"  => _ < _
    case ">"  => _ > _
    case "="  => _ == _
    case "<=" => _ <= _
    case ">=" => _ >= _
    case "!=" => _ != _
  }

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
