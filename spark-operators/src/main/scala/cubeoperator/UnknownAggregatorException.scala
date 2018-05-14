package cubeoperator

final case class UnknownAggregatorException(private val msg: String)
  extends IllegalArgumentException(msg)
