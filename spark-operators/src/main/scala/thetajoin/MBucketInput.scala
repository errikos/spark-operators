package thetajoin

/** Case class modelling a reducer bucket.
  *
  * @param hStart horizontal start
  * @param hEnd horizontal end (non-inclusive)
  * @param vStart vertical start
  * @param vEnd vertical end (non-inclusive)
  * @param numCandidates number of candidate cells in this bucket
  */
case class Bucket(hStart: Long, hEnd: Long, vStart: Long, vEnd: Long, numCandidates: Int) {
  val area: Long = math.abs(hEnd - hStart) * math.abs(vEnd - vStart)

  private def intersectsWithRow(rowId: Long): Boolean = hStart <= rowId && rowId < hEnd
  private def intersectsWithColumn(colId: Long): Boolean = vStart <= colId && colId < vEnd

  def intersectsWith(idx: Long, axis: Int): Boolean = axis match {
    case 0 => intersectsWithRow(idx)
    case 1 => intersectsWithColumn(idx)
    case _ => throw new IllegalArgumentException(s"Bucket: unsupported axis: $axis")
  }
}

/** Case class modelling a histogram region.
  *
  * @param hStart horizontal start
  * @param hEnd horizontal end (non-inclusive)
  * @param vStart vertical start
  * @param vEnd vertical end (non-inclusive)
  * @param isCandidate whether the contents of this region are candidates for the theta-join
  */
case class Region(hStart: Long, hEnd: Long, vStart: Long, vEnd: Long, isCandidate: Boolean) {
  val area: Long = math.abs(hEnd - hStart) * math.abs(vEnd - vStart)

  /** Given the "coordinates" of a bucket, estimates the number of candidates within the
    * intersection of the given bucket and this region.
    *
    * @param h1 horizontal start of the bucket
    * @param h2 horizontal end of the bucket (non-inclusive)
    * @param v1 vertical start of the bucket
    * @param v2 vertical end of the bucket (non-inclusive)
    * @return
    */
  def estimateCandidates(h1: Long, h2: Long, v1: Long, v2: Long): Int = {
    if (h2 <= hStart || hEnd <= h1 || v2 <= vStart || vEnd <= v1)
      0 // if there is no intersection, then #candidates is zero
    else {
      val ih1 = math.max(h1, hStart)
      val ih2 = math.min(h2, hEnd)
      val iv1 = math.max(v1, vStart)
      val iv2 = math.min(v2, vEnd)
      (math.abs(ih1 - ih2) * math.abs(iv1 - iv2)).toInt
    }
  }
}

/** Class implementing the M-Bucket-Input heuristic-based algorithm:
  * [[https://dl.acm.org/citation.cfm?id=1989423]]
  *
  * @param rows the number of rows
  * @param hBounds the row-based histogram boundaries
  * @param hCounts the row-based histogram counts
  * @param columns the number of columns
  * @param vBounds the column-based histogram boundaries
  * @param vCounts the column-based histogram counts
  * @param maxInput the maximum bucket size
  */
class MBucketInput(val rows: Long, // the number of rows
                   val hBounds: Seq[Int], // the row-based histogram boundaries
                   val hCounts: Seq[Long], // the row-based histogram counts
                   val columns: Long, // the number of columns
                   val vBounds: Seq[Int], // the column-based histogram boundaries
                   val vCounts: Seq[Long], // the column-based histogram counts
                   val maxInput: Int,
                   val op: String) // maximum bucket size
    extends Serializable {

  // Computes the intersection of regions `r1` and `r2`.
  private def intervalIntersection(r1: (Int, Int), r2: (Int, Int)): Option[(Int, Int)] = {
    val lower = math.max(r1._1, r2._1)
    val upper = math.min(r1._2, r2._2)
    if (lower < upper) Some(lower, upper) else Option.empty
  }

  // Determines whether `op` can hold for any elements within regions `r1` and `r2`.
  private def evalOp(r1: (Int, Int), r2: (Int, Int)): Boolean = op match {
    case "=" if intervalIntersection(r1, r2).isEmpty => false
    case "<" | "<=" if r1._1 >= r2._2                => false
    case ">" | ">=" if r1._2 <= r2._1                => false
    case _                                           => true
  }

  // horizontal accumulated boundaries
  private val hBoundsAcc = (Seq(0l) /: hCounts) {
    case (soFar, count) => soFar :+ count + soFar.last
  }
  // vertical accumulated boundaries
  private val vBoundsAcc = (Seq(0l) /: vCounts) {
    case (soFar, count) => soFar :+ count + soFar.last
  }

  /** 2-D array of [[Region]] objects.
    * Each object contains the row/column offsets of the region and whether it can
    * contain candidate elements for the join predicate.
    */
  private val candidateRegions = {
    hBounds // take the horizontal bounds
      .zip(hBoundsAcc) // along with the horizontal cumulative bounds
      .sliding(2) // start a sliding window of size 2 in horizontal regions
      .map {
        case Seq((h1, ha1), (h2, ha2)) => // for each horizontal region
          vBounds // take the vertical bounds
            .zip(vBoundsAcc) // along with the vertical cumulative bounds
            .sliding(2) // start a sliding window of size 2 in vertical regions
            .map {
              case Seq((v1, va1), (v2, va2)) => // for each vertical region
                // determine whether the predicate can hold
                Region(ha1, ha2, va1, va2, evalOp((h1, h2), (v1, v2)))
            }
            .toList
      }
      .toList
  }

  /** Computes the number of candidates within the bucket defined by the given coordinates.
    *
    * @param hStart the horizontal start of the bucket
    * @param hEnd the horizontal end of the bucket (non-inclusive)
    * @param vStart the vertical start of the bucket
    * @param vEnd the vertical end of the bucket (non-inclusive)
    * @return the number of candidates of the bucket
    */
  private def numCandidates(hStart: Long, hEnd: Long, vStart: Long, vEnd: Long): Int =
    (0 /: candidateRegions) { // fold the region rows, starting with a counter set to zero
      case (soFar, regionRow) => // for each region row
        regionRow.map { // for each region within the region row
          // if region is evaluated to false, then there are no candidates
          case region if !region.isCandidate => 0
          // if region is evaluated to true, then we need to find its intersection with the bucket
          case region if region.isCandidate =>
            region.estimateCandidates(hStart, hEnd, vStart, vEnd)
        }.sum + soFar // sum the candidates in this region row and add to the 'soFar' counter
    }

  /** For a given range of rows, finds a cover with that width.
    * Maps to Algorithm 5 from the paper.
    */
  def coverRows(rowFrom: Long, rowTo: Long): Seq[Bucket] = {
    val dx = math.abs(rowTo - rowFrom) // number of rows we are considering
    val dyMax = maxInput / dx // maximum number of columns we can take
    ((Seq.empty[Bucket], 0l) /: (1l to columns)) {
      // fold the matrix columns, with column 0 as the start and with an empty sequence of buckets
      case ((soFar, start), col) if math.abs(col - start) == dyMax || col == columns =>
        // if we reach the maximum number of columns or if we exhaust all columns
        val candidates = numCandidates(rowFrom, rowTo, start, col) // compute the candidate count
        if (candidates > 0) // if bucket contains at least one candidate, take it
          (soFar :+ Bucket(rowFrom, rowTo, start, col, candidates), col)
        else // else, leave it
          (soFar, col)
      case ((soFar, start), _) => (soFar, start)
    }._1
  }

  /** For a given start row, tries to find the best cover consisting of width = 1, 2, ..., maxInput.
    * Maps to Algorithm 4 from the paper.
    */
  def coverSubMatrix(startRow: Long): (Seq[Bucket], Long) = {
    val iMax = math.min(maxInput, rows - startRow)
    var maxScore = ((Seq.empty[Bucket], startRow + iMax), 0.0)
    for (i <- 1l to iMax if maxInput % i == 0) {
      // for each possible row block starting from startRow
      val cover = coverRows(startRow, startRow + i) // find cover with width (endRow - startRow + 1)
      val score = ((cover, startRow + i), cover.map(_.numCandidates).sum.toDouble / cover.length)
      maxScore = if (score._2 > maxScore._2) score else maxScore // keep track of max score
    }
    maxScore._1 // return cover with max score
  }

  /** Tries to find a good enough cover, by applying the M-Bucket-I algorithm.
    * Maps to Algorithm 3 from the paper.
    */
  def coverMatrix: Seq[Bucket] = {
    var row = 0l // start from row 0
    var matrixCover = Seq.empty[Bucket] // with an empty cover
    while (row < rows) { // while there are uncovered rows
      println(s"row: $row")
      // start from row and find a good enough cover using our heuristics
      val (rowCover, nextRow) = coverSubMatrix(row)
      matrixCover ++= rowCover // update result with the found cover
      row = nextRow // next cover attempt will start from nextRow
    }
    matrixCover
  }

}

object MBucketInput {
  def apply(rows: Long,
            hBounds: Seq[Int],
            hCounts: Seq[Long],
            columns: Long,
            vBounds: Seq[Int],
            vCounts: Seq[Long],
            maxInput: Int,
            op: String) =
    new MBucketInput(rows, hBounds, hCounts, columns, vBounds, vCounts, maxInput, op)
}
