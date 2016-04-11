package net.tixxit.delimited
package parser

/**
 * An immutable window into a stream of character data. The size of the window
 * is determined by 2 methods: `marked(pos)` and `append(chunk)`, which shrink
 * and expand the window respectively. All positions used with `Input` are
 * always relative to the entire stream, though only positions within the
 * current window are valid at any given point in time.
 *
 * The method `marked` returns a new `Input` which may drop all data before the
 * marked position. Accessing data before the marked position in the returned
 * `Input` is undefined and should be expected to fail in hilarious ways.
 *
 * The method `append` is used to append new chunks of data from the stream
 * onto the input. The `Input` returned by `append` will have all its data
 * up-to `mark` removed.
 *
 * @param offset the offset, in chars, of the start of `data` in the stream
 * @param data   the currently readable window into the stream
 * @param isLast true if this window covers the end of the stream
 * @param mark   the position in the stream we are able to truncate data to
 */
final class Input private (
  val offset: Long,
  val data: String,
  val isLast: Boolean,
  val mark: Long
) {
  private def check(i: Long): Int = if ((i < offset) || (i > (offset + data.length))) {
    throw new IndexOutOfBoundsException(i.toString)
  } else {
    val j = i - offset
    if (j <= Int.MaxValue) {
      j.toInt
    } else {
      throw new IndexOutOfBoundsException(i.toString)
    }
  }

  /**
   * Returns the character data between `mark` and `limit`. This is equivalent
   * to calling `input.substring(input.mark, input.limit)`.
   */
  def window: String = substring(math.min(limit, mark), limit)

  /**
   * Returns the character at the given position. This method will do some
   * aggressive bounds checking.
   */
  def charAt(i: Long): Char = data.charAt(check(i))

  /**
   * Returns the index of the first position that cannot be read. If `isLast`
   * is `true`, then this is the length of the stream.
   */
  def limit: Long = offset + data.length

  /**
   * Returns the substring between `from` (inclusive) and `until` (exclusive).
   * It is expected that `mark <= from <= until <= limit`.
   */
  def substring(from: Long, until: Long): String =
    data.substring(check(from), check(until))

  /**
   * Returns an `Input` whose `mark` is at the given position.
   */
  def marked(pos: Long): Input = {
    if (pos < mark) {
      throw new IllegalArgumentException("mark cannot be smaller than current mark")
    }
    new Input(offset, data, isLast, pos)
  }

  private def trim: Input = {
    val next = math.min(mark - offset, data.length.toLong).toInt
    val tail = data.substring(next)
    val offset0 = offset + next
    new Input(offset0, tail, isLast, mark)
  }

  /**
   * Returns an `Input` with the chunk of data appended to the currently
   * readable window. If `last` is true, then the returned `Input`'s `isLast`
   * method returns `true`. Any data available in the current `Input`, but
   * prior to this `Input`'s `mark` will not be available in the returned
   * `Input`.
   */
  def append(chunk: String, last: Boolean = false): Input =
    if (mark > offset && data.length > 0) trim.append(chunk, last)
    else if (chunk.isEmpty) new Input(offset, data, last, mark)
    else new Input(offset, data + chunk, last, mark)

  /**
   * Returns a copy of this `Input` where `isLast` is true.
   */
  def finished: Input = new Input(offset, data, true, mark)
}

object Input {
  def init(str: String): Input =
    new Input(0, str, false, 0)
}
