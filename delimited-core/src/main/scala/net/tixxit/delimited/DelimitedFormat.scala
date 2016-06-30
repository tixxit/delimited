package net.tixxit.delimited

import java.io.{ Reader, PushbackReader }
import java.util.regex.Pattern

/**
 * There are 2 types of `DelimitedFormatStrategy`s: [[GuessDelimitedFormat]]
 * and [[DelimitedFormat]]. A [[DelimitedFormat]] is delimited format that is
 * completely specified. It can actually be used to render and parse delimited
 * files without further work. On the other hand, [[GuessDelimitedFormat]] may
 * have any number of parameters left unspecified, which means they need to be
 * inferred before the format can be used to parse/render a delimited file.
 *
 * All the method provided in `DelimitedFormatStrategy` are ways of fixing (or
 * updating) the various parameters used. In the case of a [[DelimitedFormat]],
 * this just changes that parameter and keeps all others the same. In the case
 * of [[GuessDelimitedFormat]], it will fix that parameter, so it no longer
 * needs to be inferred.
 */
sealed trait DelimitedFormatStrategy {
  def withSeparator(separator: String): DelimitedFormatStrategy
  def withQuote(quote: String): DelimitedFormatStrategy
  def withQuoteEscape(quoteEscape: String): DelimitedFormatStrategy
  def withRowDelim(rowDelim: RowDelim): DelimitedFormatStrategy
  def withRowDelim(rowDelim: String): DelimitedFormatStrategy
  def withRowDelimInQuotes(allowRowDelimInQuotes: Boolean): DelimitedFormatStrategy
}

/**
 * A [[DelimitedFormatStrategy]] that can infer some or all parameters of a
 * [[DelimitedFormat]] given an adequate sample of a delimited file.
 */
trait GuessDelimitedFormat extends DelimitedFormatStrategy {

  /**
   * Makes a guess at the format of the CSV accessed by `reader`. This returns
   * the format, as well as the a new pushback reader to be used in place of
   * `reader`. The original reader will have some data read out of it. The
   * returned reader will contain all the original reader's data.
   */
  def apply(reader: Reader): (DelimitedFormat, Reader) = {
    val reader0 = new PushbackReader(reader, DelimitedParser.BufferSize)
    val buffer = new Array[Char](DelimitedParser.BufferSize)
    val len = reader0.read(buffer)
    if (len < 0) {
      (apply(""), reader)
    } else {
      reader0.unread(buffer, 0, len)
      val chunk = new String(buffer, 0, len)
      val format = apply(chunk)
      (format, reader0)
    }
  }

  /**
   * Given the first part of a CSV file, return a guess at the format.
   */
  def apply(str: String): DelimitedFormat
}

/**
 * A [[DelimitedFormatStrategy]] where all parameters have been completely
 * fixed.
 *
 * @param separator             the delimiter that separates fields within a row
 * @param quote                 the character/string that indicates the
 *                              beginning/end of a quoted value
 * @param quoteEscape           the string that is used to escape a quote
 *                              character, within a quoted value
 * @param rowDelim              the delimiter used to separate rows
 * @param allowRowDelimInQuotes if true, allow row delimiters within quotes,
 *                              otherwise they are treated as an error
 */
case class DelimitedFormat(
  separator: String,
  quote: String = "\"",
  quoteEscape: String = "\"",
  rowDelim: RowDelim = RowDelim.Both,
  allowRowDelimInQuotes: Boolean = true
) extends DelimitedFormatStrategy {
  private[this] val primaryRowDelim: String = rowDelim.value
  private[this] val secondaryRowDelim: String = rowDelim.alternate.orNull

  /**
   * Returns an escaped quote that can be used to represent a literal quote
   * within a quoted value.
   */
  val escapedQuote: String = quoteEscape + quote

  /**
   * Replaces all escaped quoted in a quoted value with literal quotes
   */
  def unescape(value: String): String =
    value.replace(escapedQuote, quote)

  /**
   * Escapes all quotes.
   */
  def escape(text: String): String =
    text.replace(quote, escapedQuote)

  /**
   * If `text` starts with a quote, then this removes the wrapping quotes, then
   * unescapes the resulting text. If text does not start with a quote, then it
   * is returned unchanged.
   *
   * This is the opposite of `render`.
   */
  def unquote(text: String): String =
    if (text.startsWith(quote)) {
      unescape(text.substring(quote.length, text.length - quote.length))
    } else {
      text
    }

  private def match1(text: String, i: Int, value: String): Boolean = {
    var j = i + 1
    var k = 1
    while (k < value.length &&
           j < text.length &&
           text.charAt(j) == value.charAt(k)) {
      j += 1
      k += 1
    }
    k == value.length
  }

  private def mustEscape(text: String): Boolean = {
    var i = 0
    while (i < text.length) {
      val ch = text.charAt(i)
      if (ch == separator.charAt(0) && match1(text, i, separator))
        return true
      if (ch == quote.charAt(0) && match1(text, i, quote))
        return true
      if (ch == primaryRowDelim.charAt(0) && match1(text, i, primaryRowDelim))
        return true
      if (secondaryRowDelim != null &&
          ch == secondaryRowDelim.charAt(0) && match1(text, i, secondaryRowDelim))
        return true
      i += 1
    }
    false
  }

  /**
   * Renders a single cell of data, quoting and escaping the value if
   * necessary. A cell is quoted and escaped if it contains a row delimiter,
   * the separator, or a quote.
   */
  def render(text: String): String = {
    if (mustEscape(text)) {
      new java.lang.StringBuilder(quote)
        .append(escape(text))
        .append(quote)
        .toString
    } else {
      text
    }
  }

  def withSeparator(separator: String): DelimitedFormat = copy(separator = separator)
  def withQuote(quote: String): DelimitedFormat = copy(quote = quote)
  def withQuoteEscape(quoteEscape: String): DelimitedFormat = copy(quoteEscape = quoteEscape)
  def withRowDelim(rowDelim: RowDelim): DelimitedFormat = copy(rowDelim = rowDelim)
  def withRowDelim(rowDelim: String): DelimitedFormat = copy(rowDelim = RowDelim(rowDelim))
  def withRowDelimInQuotes(allowRowDelimInQuotes: Boolean): DelimitedFormat = copy(allowRowDelimInQuotes = allowRowDelimInQuotes)

  override def toString: String =
    s"""DelimitedFormat(separator = "$separator", quote = "$quote", quoteEscape = "$quoteEscape", rowDelim = $rowDelim, allowRowDelimInQuotes = $allowRowDelimInQuotes)"""
}

object DelimitedFormat {

  /**
   * A [[DelimitedFormat]] using the following parameters:
   *
   * {{{
   * val CSV = RowDelim(
   *   separator = ","
   *   quote = "\""
   *   quoteEscape = "\""
   *   rowDelim = RowDelim.Both, // \n, but also accept \r\n during parsing
   *   allowRowDelimInQuotes = true
   * )
   * }}}
   */
  val CSV = DelimitedFormat(",")

  /**
   * A [[DelimitedFormat]] using the following parameters:
   *
   * {{{
   * val TSV = RowDelim(
   *   separator = "\t"
   *   quote = "\""
   *   quoteEscape = "\""
   *   rowDelim = RowDelim.Both, // \n, but also accept \r\n during parsing
   *   allowRowDelimInQuotes = true
   * )
   * }}}
   */
  val TSV = DelimitedFormat("\t")

  /**
   * A [[DelimitedFormatStrategy]] that infers *all* parameters in
   * [[DelimitedFormat]].
   */
  val Guess = PartialFormat()
}
