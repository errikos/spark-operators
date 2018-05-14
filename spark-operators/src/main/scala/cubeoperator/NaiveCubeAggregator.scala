package cubeoperator

import cubeoperator.Utils.MaskGenerator
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

sealed abstract class NaiveCubeAggregator(val name: String) extends Product with Serializable {
  type Key = String
  type Value

  // these implicit values are needed by spark reduceByKey, so that it can know the Key/Value types
  implicit val keyClassTag: ClassTag[Key] = ClassTag(classOf[Key])
  implicit val valueClassTag: ClassTag[Value]

  // core cube aggregator functions
  def mapper(row: Row): TraversableOnce[(Key, Value)]
  def reducer(v1: Value, v2: Value): Value
  // the last step that an operator has to do;
  // necessary for AVG-like operators as well as to convert to Double
  def epilogueMapper(v: Value): Double

  final def generateMappingsWithValue(row: Row, v: Value): TraversableOnce[(Key, Value)] = {
    for (mask <- MaskGenerator(row.length))
      yield
        (row.toSeq
           .zip(mask)
           .map { case (a, b) => if (b) a else "ALL" }
           .mkString(";"),
         v)
  }

  /**
    * Returns a new Row containing the attributes from 'row' that
    * correspond to the indexes found in 'idx'.
    */
  def selectAttrs(row: Row, idx: Seq[Int]): Row =
    Row.fromSeq(idx.map { row(_) })

  override def toString: String = name
}

object NaiveCubeAggregator {
  def apply(agg: String, keyIdx: Seq[Int], valIdx: Int): NaiveCubeAggregator =
    agg match {
      case "COUNT" => NaiveCount(keyIdx)
      case "SUM"   => NaiveSum(keyIdx, valIdx)
      case "MIN"   => NaiveMin(keyIdx, valIdx)
      case "MAX"   => NaiveMax(keyIdx, valIdx)
      case "AVG"   => NaiveAvg(keyIdx, valIdx)
      case _       => throw UnknownAggregatorException(s"Unknown naive aggregator: $agg")
    }
}

case class NaiveCount(private val keyIdx: Seq[Int]) extends NaiveCubeAggregator("NAIVE_COUNT") {
  override type Value = Double
  override implicit val valueClassTag: ClassTag[Value] = ClassTag(classOf[Value])

  override def mapper(row: Row): TraversableOnce[(Key, Value)] =
    generateMappingsWithValue(selectAttrs(row, keyIdx), 1)
  override def reducer(v1: Value, v2: Value): Value = v1 + v2
  override def epilogueMapper(v: Value): Double = v
}

case class NaiveSum(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends NaiveCubeAggregator("NAIVE_SUM") {
  override type Value = Double
  override implicit val valueClassTag: ClassTag[Value] = ClassTag(classOf[Value])

  override def mapper(row: Row): TraversableOnce[(Key, Value)] =
    generateMappingsWithValue(selectAttrs(row, keyIdx), row.getInt(valIdx))
  override def reducer(v1: Value, v2: Value): Value = v1 + v2
  override def epilogueMapper(v: Value): Double = v
}

case class NaiveMin(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends NaiveCubeAggregator("NAIVE_MIN") {
  override type Value = Double
  override implicit val valueClassTag: ClassTag[Value] = ClassTag(classOf[Value])

  override def mapper(row: Row): TraversableOnce[(Key, Value)] =
    generateMappingsWithValue(selectAttrs(row, keyIdx), row.getInt(valIdx))
  override def reducer(v1: Value, v2: Value): Value = math.min(v1, v2)
  override def epilogueMapper(v: Value): Double = v
}

case class NaiveMax(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends NaiveCubeAggregator("NAIVE_MAX") {
  override type Value = Double
  override implicit val valueClassTag: ClassTag[Value] = ClassTag(classOf[Value])

  override def mapper(row: Row): TraversableOnce[(Key, Value)] =
    generateMappingsWithValue(selectAttrs(row, keyIdx), row.getInt(valIdx))
  override def reducer(v1: Value, v2: Value): Value = math.max(v1, v2)
  override def epilogueMapper(v: Value): Double = v
}

case class NaiveAvg(private val keyIdx: Seq[Int], private val valIdx: Int)
    extends NaiveCubeAggregator("NAIVE_AVG") {
  override type Value = (Double, Int)
  override implicit val valueClassTag: ClassTag[Value] = ClassTag(classOf[Value])

  override def mapper(row: Row): TraversableOnce[(Key, Value)] =
    generateMappingsWithValue(selectAttrs(row, keyIdx), (row.getInt(valIdx), 1))
  override def reducer(v1: Value, v2: Value): Value = (v1._1 + v2._1, v1._2 + v2._2)
  override def epilogueMapper(v: Value): Double = v._1 / v._2
}
