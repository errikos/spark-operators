package streaming

import org.apache.spark.streaming.dstream.DStream

sealed abstract class StreamOperator extends Product with Serializable {
  def setup()
}

object StreamOperator {
  def apply(strategy: String, stream: DStream[(String, String)]): StreamOperator =
    strategy match {
      case "precise" => PreciseStreamOperator(stream)
      case "approx"  => ApproximateStreamOperator(stream)
      case _         => throw UnknownStrategyException(s"Unknown strategy: $strategy")
    }
}

case class PreciseStreamOperator(stream: DStream[(String, String)]) extends StreamOperator {
  override def setup(): Unit = ???
}

case class ApproximateStreamOperator(stream: DStream[(String, String)]) extends StreamOperator {
  override def setup(): Unit = ???
}
