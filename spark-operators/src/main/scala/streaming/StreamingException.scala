package streaming

trait StreamingException extends Exception

final case class UnknownStrategyException(private val msg: String)
    extends IllegalArgumentException(msg)
    with StreamingException
