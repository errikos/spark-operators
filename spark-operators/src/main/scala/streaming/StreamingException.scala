package streaming

trait StreamingException extends Exception

final case class UnknownStrategyException(private val msg: String)
    extends IllegalArgumentException(msg)
    with StreamingException

final case class MissingArgumentException(private val arg: String)
    extends IllegalArgumentException(s"Missing argument: $arg")
    with StreamingException
