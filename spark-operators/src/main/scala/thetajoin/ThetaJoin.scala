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

  /** For a given RDD, bucket list and axis, returns a new RDD, which has a bucket ID
    * (i.e. reducer ID) set as the key and a (row value, axis) pair as value.
    *
    * Warning: This method calls [[RDD.sortBy]] on the input RDD.
    *
    * @param rdd the RDD whose rows to assign to buckets
    * @param buckets the bucket list, containing instances of [[Bucket]]
    * @param axis the intersection axis: 0 for horizontal, 1 for vertical
    * @return a new RDD with the corresponding bucket ID as the key in each row
    */
  def assignBuckets(rdd: RDD[Int], buckets: Seq[Bucket], axis: Int): RDD[(Int, (Int, Int))] = {
    rdd
      .sortBy(v => v, numPartitions = reducers) // sort the given RDD
      .zipWithIndex // take each row with its row ID
      .flatMap {
        case (v, idx) => // for each (value, rowId) pair
          // calculate the buckets it intersects with in the given axis
          (Seq.empty[(Int, (Int, Int))] /: buckets.zipWithIndex) {
            case (soFar, (bucket, bucket_idx)) =>
              if (bucket.intersectsWith(idx, axis = axis))
                soFar :+ (bucket_idx, (v, axis))
              else
                soFar
          }
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
    // enclose the bounds with Int.MinValue and Int.MaxValue to "complete" the intervals
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
    val mbi = MBucketInput(numR, R_boundsEncl, R_counts, numS, S_boundsEncl, S_counts, bucketSize, op)
    val buckets = mbi.coverMatrix // create an M-Bucket-I object and calculate the buckets

    println("Assigning values to buckets...")

    // (c) assign the values of each data set (R, S) to the corresponding bucket ------------------
    val R_assigned = assignBuckets(R, buckets, axis = 0)
    val S_assigned = assignBuckets(S, buckets, axis = 1)

    println("Grouping and performing join...")

    (R_assigned union S_assigned)
      // (d) partition the data sets using as key the corresponding bucket number -----------------
      .groupByKey(reducers)
      // (e) perform the theta-join operation locally in each partition ---------------------------
      .flatMapValues { tuples =>
        val split = splitTuples(tuples)
        |><|(split._1, split._2, ThetaJoin.getOp(op))
      }.values
  }

//    R_assigned
//      .map { case (key, value) => (key, Seq(value)) }
//      .union(S_assigned.map { case (key, value) => (key, Seq(value)) })
      // (d) partition the data sets using as key the corresponding bucket number -----------------
//      .reduceByKey(_ ++ _, numPartitions = reducers)
      // (e) perform the theta-join operation locally in each partition ---------------------------
//      .flatMapValues { thisBucketTuples =>
//        val split = splitTuples(thisBucketTuples)
//        |><|(split._1, split._2, ThetaJoin.getOp(op))
//      }
//      .values

  /** Given an iterable of (value, relationId) tuples, returns a Tuple2 of iterables
    * where the first iterable contains the values of relation #1 and the second iterable
    * contains the values of relation #2.
    */
  private def splitTuples(tuples: Iterable[(Int, Int)]): (Iterable[Int], Iterable[Int]) = {
    ((Seq.empty[Int], Seq.empty[Int]) /: tuples) {
      case (soFar, (v, 0)) => (soFar._1 :+ v, soFar._2)
      case (soFar, (v, 1)) => (soFar._1, soFar._2 :+ v)
    }
  }

  /** Performs a simple nested-loop join on the values of `dat1` and `dat2`,
    * using `op` as the join predicate.
    *
    * @param dat1 the first data set
    * @param dat2 the second data set
    * @param op the join predicate
    * @return a list containing the resulting pairs from the join
    */
  private def |><|(dat1: Iterable[Int],
                   dat2: Iterable[Int],
                   op: (Int, Int) => Boolean): TraversableOnce[(Int, Int)] = {
    val res = scala.collection.mutable.MutableList.empty[(Int, Int)]
    for (t1 <- dat1)
      for (t2 <- dat2)
        if (op(t1, t2))
          res += ((t1, t2))
    res
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
}
