package cubeoperator

object Utils {

  /**
    * Generates all possible masks of the given length.
    */
  class MaskGenerator(val length: Int) extends Iterable[Seq[Boolean]] {
    // head of mask is initially false, so that hasNext succeeds;
    // in the first iteration, it becomes true and stays that way;
    // at the same time, every other bit becomes false and gives the first result
    private var mask = false +: Seq.fill(length + 1)(true)

    override def iterator: Iterator[Seq[Boolean]] = new Iterator[Seq[Boolean]] {
      override def hasNext: Boolean = mask.contains(false)
      override def next(): Seq[Boolean] = {
        mask = (mask :\ (Seq.empty[Boolean], true)) {
          case (elem, (soFar, b)) =>
            if (!b) (elem +: soFar, b)
            else if (elem ^ b) (true +: soFar, !b)
            else (false +: soFar, b)
        }._1
        mask.tail  // tail is what we are interested in
      }
    }
  }

  object MaskGenerator {
    def apply(length: Int) = new MaskGenerator(length)
  }

}
