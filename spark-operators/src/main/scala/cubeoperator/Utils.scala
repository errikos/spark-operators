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
        // fold mask from the right; start with an empty Seq and a true value;
        // the true value represents the +1 we want to add to the binary representation,
        // that is, the carry in each step of the fold
        mask = (mask :\ (Seq.empty[Boolean], true)) {
          case (elem, (soFar, b)) =>
            // no carry, continue by prepending the current element
            if (!b) (elem +: soFar, b)
            // carry added to this 'bit' (which was false), set and continue with no carry
            else if (elem ^ b) (true +: soFar, !b)
            // carry not added to this 'bit' (which was true), unset and continue with carry
            else (false +: soFar, b)
        }._1
        mask.tail // tail is what we are interested in
      }
    }
  }

  object MaskGenerator {
    def apply(length: Int) = new MaskGenerator(length)
  }

}
