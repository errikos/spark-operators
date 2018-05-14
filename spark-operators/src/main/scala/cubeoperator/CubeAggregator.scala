package cubeoperator

import cubeoperator.Utils.MaskGenerator
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

/**
  * Cube aggregator base abstract class.
  *
  * The various cube operators are defined as subclasses of this class.
  * Key and Value types are parameterised, with Key being preset to Row.
  * Any subclass:
  *   - must define the Value type, as well as the valueClassTag value;
  *   - can redefine the Key type, in which case it also has to redefine the keyClassTag value.
  */
sealed abstract class CubeAggregator(val name: String) extends Product with Serializable {
  type Key = Row
  type Value

  // these implicit values are needed by spark reduceByKey, so that it can know the Key/Value types
  implicit val keyClassTag: ClassTag[Key] = ClassTag(classOf[Key])
  implicit val valueClassTag: ClassTag[Value]

  // core cube aggregator functions
  def mapper(row: Row): (Key, Value)
  def reducer(v1: Value, v2: Value): Value
  // generates the partial upper cells (end of phase 1)
  final def partialGenerator(row: (Key, Value)): TraversableOnce[(Key, Value)] = {
    val attrs = row._1
    for (mask <- MaskGenerator(attrs.length))
      yield (Row(attrs.toSeq.zip(mask).map { case (a, b) => if (b) a else '*' }), row._2)
  }

  /**
    * Returns a new Row containing the attributes from 'row' that
    * correspond to the indexes found in 'idx'.
    */
  def selectAtts(row: Row, idx: Seq[Int]): Row =
    Row.fromSeq(idx.map { row(_) })

  override def toString: String = name
}

/**
  * Aggregator companion object for Aggregator class above.
  * Apply in order to get the appropriate Aggregator object.
  */
object CubeAggregator {
  def apply(agg: String, keyIdx: Seq[Int], valIdx: Int): CubeAggregator =
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
case class Count(private val keyIdx: Seq[Int]) extends CubeAggregator("COUNT") {
  override type Value = Double
  override implicit val valueClassTag: ClassTag[Value] = ClassTag(classOf[Value])

  override def mapper(row: Row): (Key, Value) = (selectAtts(row, keyIdx), 1)
  override def reducer(v1: Value, v2: Value): Value = v1 + v2
}

// Sum operator; sums a specific row attribute (may not be in key).
case class Sum(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends CubeAggregator("SUM") {
  override type Value = Double
  override implicit val valueClassTag: ClassTag[Value] = ClassTag(classOf[Value])

  override def mapper(row: Key): (Key, Value) = (selectAtts(row, keyIdx), row.getInt(valIdx))
  override def reducer(v1: Value, v2: Value): Value = v1 + v2
}

// Min operator; finds the minimum value of an attribute (may not be in key).
case class Min(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends CubeAggregator("MIN") {
  override type Value = Double
  override implicit val valueClassTag: ClassTag[Value] = ClassTag(classOf[Value])

  override def mapper(row: Key): (Key, Value) = (selectAtts(row, keyIdx), row.getInt(valIdx))
  override def reducer(v1: Value, v2: Value): Value = math.min(v1, v2)
}

// Max operator; finds the maximum value of an attribute (may not be in key).
case class Max(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends CubeAggregator("MAX") {
  override type Value = Double
  override implicit val valueClassTag: ClassTag[Value] = ClassTag(classOf[Value])

  override def mapper(row: Key): (Key, Value) = (selectAtts(row, keyIdx), row.getInt(valIdx))
  override def reducer(v1: Value, v2: Value): Value = math.max(v1, v2)
}

// Avg operator; finds the average value of an attribute (may not be in key).
case class Avg(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends CubeAggregator("AVG") {
  override type Value = (Double, Int)
  override implicit val valueClassTag: ClassTag[Value] = ClassTag(classOf[Value])

  override def mapper(row: Key): (Key, Value) = (selectAtts(row, keyIdx), (row.getInt(valIdx), 1))
  override def reducer(v1: Value, v2: Value): Value = (v1._1 + v2._1, v1._2 + v2._2)
}
