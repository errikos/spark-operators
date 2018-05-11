package cubeoperator

import org.apache.spark.sql.Row

sealed abstract class Aggregator(name: String) {
  type Key = Row
  type Value = Double

  def mapper(row: Key): (Key, Value)
  def reducer(v1: Value, v2: Value): Value

  /**
    * Returns a new Row containing the attributes from 'row' that
    * correspond to the indexes found in 'idx'.
    */
  def selectAtts(row: Key, idx: Seq[Int]): Key =
    Row.fromSeq(idx.map { row(_) })

  override def toString: String = name
}

/**
  * Aggregator companion object for Aggregator class above.
  * Apply in order to get the appropriate Aggregator object.
  */
object Aggregator {
  def apply(agg: String, keyIdx: Seq[Int], valIdx: Int): Aggregator =
    agg match {
      case "COUNT" => Count(keyIdx)
      case "SUM"   => Sum(keyIdx, valIdx)
      case "MIN"   => Min(keyIdx, valIdx)
      case "MAX"   => Max(keyIdx, valIdx)
      case "AVG"   => Avg(keyIdx, valIdx)
      case _       => throw UnknownAggregatorException(s"Unknown aggregator: $agg")
    }
}

final case class UnknownAggregatorException(private val msg: String)
    extends IllegalArgumentException(msg)

// Count operator; counts the occurrences of each key.
case class Count(private val keyIdx: Seq[Int]) extends Aggregator("COUNT") {
  override def mapper(row: Key): (Key, Value) = (selectAtts(row, keyIdx), 1)
  override def reducer(v1: Value, v2: Value): Value = v1 + v2
}

// Sum operator; sums a specific row attribute (may not be in key).
case class Sum(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends Aggregator("SUM") {
  override def mapper(row: Key): (Key, Value) = (selectAtts(row, keyIdx), row.getDouble(valIdx))
  override def reducer(v1: Value, v2: Value): Value = v1 + v2
}

// Min operator; finds the minimum value of an attribute (may not be in key).
case class Min(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends Aggregator("MIN") {
  override def mapper(row: Key): (Key, Value) = (selectAtts(row, keyIdx), row.getDouble(valIdx))
  override def reducer(v1: Value, v2: Value): Value = math.min(v1, v2)
}

// Max operator; finds the maximum value of an attribute (may not be in key).
case class Max(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends Aggregator("MAX") {
  override def mapper(row: Key): (Key, Value) = (selectAtts(row, keyIdx), row.getDouble(valIdx))
  override def reducer(v1: Value, v2: Value): Value = math.max(v1, v2)
}

// Avg operator; finds the average value of an attribute (may not be in key).
case class Avg(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends Aggregator("AVG") {
  override def mapper(row: Key): (Key, Value) = (selectAtts(row, keyIdx), row.getDouble(valIdx))
  override def reducer(v1: Value, v2: Value): Value = (v1 + v2) / 2
}
