package net.tixxit.delimited

/**
 * An error that happened while parsing a row from a delimted file.
 *
 * @param message  a message describing the error
 * @param rowStart the offset (# of chars), into the file, where the row starts
 * @param pos      the position (chars) in the file where the error occured
 * @param context  the text of the row, up to at least where the error occured
 * @param row      the row (0-based) where the error occured
 * @param col      the column (0-based) where the error occured
 */
case class DelimitedError(message: String, rowStart: Long, pos: Long, context: String, row: Long, col: Long) {

  /**
   * A helpful, nicely rendered, multiline message that is suitable for human
   * consumption when printed to a console with a monospaced font.
   */
  def description: String = {
    val msg = s"Error parsing CSV row: $message"
    val prefix = s"Row $row: "
    val padLength = col.toInt - 1 + prefix.length
    val pointer = (" " * padLength) + "^"

    s"$msg\n\n$prefix$context\n$pointer"
  }
}
