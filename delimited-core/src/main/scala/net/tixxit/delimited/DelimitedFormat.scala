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
    reader0.unread(buffer, 0, len)

    val chunk = new String(buffer, 0, len)
    val format = apply(chunk)
    (format, reader0)
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
   * Replaces all instances of \r\n with \n, then escapes all quotes and wraps
   * the string in quotes.
   */
  def escape(text: String): String = {
    val text0 = text.replace("\r\n", "\n").replace(quote, escapedQuote)
    new java.lang.StringBuilder(quote)
      .append(text0)
      .append(quote)
      .toString
  }

  private def match1(text: String, i: Int, value: String): Boolean = {
    var j = i + 1
    var k = 1
    while (k < separator.length &&
           j < text.length &&
           text.charAt(j) == separator.charAt(k)) {
      j += 1
      k += 1
    }
    k == separator.length
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
      escape(text)
    } else {
      text
    }
  }

  def withSeparator(separator: String): DelimitedFormat = copy(separator = separator)
  def withQuote(quote: String): DelimitedFormat = copy(quote = quote)
  def withQuoteEscape(quoteEscape: String): DelimitedFormat = copy(quoteEscape = quoteEscape)
  def withRowDelim(rowDelim: RowDelim): DelimitedFormat = copy(rowDelim = rowDelim)
  def withRowDelim(rowDelim: String): DelimitedFormat = copy(rowDelim = RowDelim.Custom(rowDelim))

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
  val Guess = Partial()

  case class Partial(
      separator: Option[String] = None,
      quote: Option[String] = None,
      quoteEscape: Option[String] = None,
      rowDelim: Option[RowDelim] = None,
      allowRowDelimInQuotes: Boolean = true
    ) extends GuessDelimitedFormat {

    def withSeparator(separator: String): Partial = copy(separator = Some(separator))
    def withQuote(quote: String): Partial = copy(quote = Some(quote))
    def withQuoteEscape(quoteEscape: String): Partial = copy(quoteEscape = Some(quoteEscape))
    def withRowDelim(rowDelim: RowDelim): Partial = copy(rowDelim = Some(rowDelim))
    def withRowDelim(rowDelim: String): Partial = copy(rowDelim = Some(RowDelim.Custom(rowDelim)))

    /**
     * Performs a very naive guess of the DelimitedFormat. This uses weighted
     * frequencies of occurences of common separators, row-delimiters, quotes,
     * quote escapes, etc. and simply selects the max for each.
     *
     * This supports:
     *
     *   * \r\n and \n as row delimiters,
     *   * ',', '\t', ';', and '|' as field delimiters,
     *   * '"', and ''' as quote delimiter,
     *   * the quote delimiter or \ for quote escapes.
     */
    def apply(str: String): DelimitedFormat = {
      def count(ndl: String): Int = {
        def check(i: Int, j: Int = 0): Boolean =
          if (j >= ndl.length) true
          else if (i < str.length && str.charAt(i) == ndl.charAt(j)) check(i + 1, j + 1)
          else false

        def loop(i: Int, cnt: Int): Int =
          if (i < str.length) {
            loop(i + 1, if (check(i)) cnt + 1 else cnt)
          } else cnt

        loop(0, 0)
      }

      def choose(weightedOptions: (String, Double)*)(f: String => Int): String = {
        val weights = Map(weightedOptions: _*)
        val (best, weight) = weights.maxBy { case (c, w) => w * f(c) }
        if (weight > 0) best else weights.maxBy(_._2)._1
      }

      val rowDelim0 = rowDelim.getOrElse {
        val windCnt = count("\r\n")
        val unixCnt = count("\n")

        if ((windCnt < 4 * unixCnt) && (unixCnt < 4 * windCnt)) RowDelim.Both
        else if (windCnt < 4 * unixCnt) RowDelim.Unix
        else RowDelim.Windows
      }
      val separator0 = separator.getOrElse {
        choose(","  -> 2.0, "\t" -> 3.0, ";"  -> 2.0, "|"  -> 1.0)(count)
      }
      val quote0 = quote.getOrElse(choose("\"" -> 1.2, "\'" -> 1)(count))
      val quoteEscape0 = choose(s"$quote0$quote0" -> 1.1, s"\\$quote0" -> 1)(count).dropRight(quote0.length)

      val cells = for {
        row0 <- str.split(Pattern.quote(rowDelim0.value))
        row <- rowDelim0.alternate.fold(Array(row0)) { alt =>
            row0.split(Pattern.quote(alt))
          }
        cell <- row.split(Pattern.quote(separator0))
      } yield cell
      def matches(value: String): Int = cells.filter(_ == value).size

      DelimitedFormat(separator0, quote0, quoteEscape0, rowDelim0, allowRowDelimInQuotes)
    }
  }
}
