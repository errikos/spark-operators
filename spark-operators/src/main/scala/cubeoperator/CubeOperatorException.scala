package cubeoperator

trait CubeOperatorException extends Exception

final case class UnknownAggregatorException(private val msg: String)
  extends IllegalArgumentException(msg) with CubeOperatorException
