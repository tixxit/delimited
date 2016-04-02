package net.tixxit.delimited

import java.io.{ Reader, PushbackReader }
import java.util.regex.Pattern

sealed abstract class RowDelim(val value: String, val alternate: Option[String] = None)
object RowDelim {
  case class Custom(delim: String) extends RowDelim(delim)
  case object Unix extends RowDelim("\n")
  case object Windows extends RowDelim("\r\n")
  case object Both extends RowDelim("\n", Some("\r\n"))
}

sealed trait DelimitedFormatStrategy {
  def withSeparator(separator: String): DelimitedFormatStrategy
  def withQuote(quote: String): DelimitedFormatStrategy
  def withQuoteEscape(quoteEscape: String): DelimitedFormatStrategy
  def withHeader(header: Boolean): DelimitedFormatStrategy
  def withRowDelim(rowDelim: RowDelim): DelimitedFormatStrategy
  def withRowDelim(rowDelim: String): DelimitedFormatStrategy
}

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

case class DelimitedFormat(
  /** The delimiter that separates fields within the rows. */
  separator: String,

  /** The character/string that indicates the beginning/end of a quoted value. */
  quote: String = "\"",

  /** The string that is used to escape a quote character, within a quote. */
  quoteEscape: String = "\"",

  /** Indicates whether or not the CSV's first row is actually a header. */
  header: Boolean = false,

  /** The delimiter used to separate row. */
  rowDelim: RowDelim = RowDelim.Both,

  /** If true, allow row delimiters within quotes, otherwise they are treated
   *  as an error. */
  allowRowDelimInQuotes: Boolean = true
) extends DelimitedFormatStrategy {
  val escapedQuote = quoteEscape + quote

  def unescape(value: String): String =
    value.replace(escapedQuote, quote)

  override def toString: String =
    s"""DelimitedFormat(separator = "$separator", quote = "$quote", quoteEscape = "$quoteEscape", header = $header, rowDelim = $rowDelim, allowRowDelimInQuotes = $allowRowDelimInQuotes)"""

  /**
   * Replaces all instances of \r\n with \n, then escapes all quotes and wraps
   * the string in quotes.
   */
  def escape(text: String): String = {
    val text0 = text.replace("\r\n", "\n").replace(quote, escapedQuote)
    s"${quote}$text0${quote}"
  }

  /**
   * Renders a single cell of data, escaping the value if necessary.
   */
  def render(text: String): String = {
    if ((text contains '\n') ||
        (text contains separator) ||
        (text contains quote)) escape(text)
    else text
  }

  def withSeparator(separator: String): DelimitedFormat = copy(separator = separator)
  def withQuote(quote: String): DelimitedFormat = copy(quote = quote)
  def withQuoteEscape(quoteEscape: String): DelimitedFormat = copy(quoteEscape = quoteEscape)
  def withHeader(header: Boolean): DelimitedFormat = copy(header = header)
  def withRowDelim(rowDelim: RowDelim): DelimitedFormat = copy(rowDelim = rowDelim)
  def withRowDelim(rowDelim: String): DelimitedFormat = copy(rowDelim = RowDelim.Custom(rowDelim))
}

object DelimitedFormat {
  val CSV = DelimitedFormat(",")
  val TSV = DelimitedFormat("\t")

  val Guess = Partial(header = Some(false))

  case class Partial(
      separator: Option[String] = None,
      quote: Option[String] = None,
      quoteEscape: Option[String] = None,
      header: Option[Boolean] = None,
      rowDelim: Option[RowDelim] = None,
      allowRowDelimInQuotes: Boolean = true
    ) extends GuessDelimitedFormat {

    def withSeparator(separator: String): Partial = copy(separator = Some(separator))
    def withQuote(quote: String): Partial = copy(quote = Some(quote))
    def withQuoteEscape(quoteEscape: String): Partial = copy(quoteEscape = Some(quoteEscape))
    def withHeader(header: Boolean): Partial = copy(header = Some(header))
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
     *
     * Headers are guessed by using the cosine similarity of the frequency of
     * characters (except quotes/field delimiters) between the first row and
     * all subsequent rows. Values below 0.5 will result in a header being
     * inferred.
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

      val header0 = header.getOrElse(hasHeader(str, rowDelim0.value, separator0, quote0))

      DelimitedFormat(separator0, quote0, quoteEscape0, header0, rowDelim0, allowRowDelimInQuotes)
    }

    private def dot[K](u: Map[K, Double], v: Map[K, Double]): Double = {
      if (u.size < v.size) {
        dot(v, u)
      } else {
        var sum = 0D
        v.foreach { case (k, y) =>
          u.get(k).foreach { x =>
            sum += x * y
          }
        }
        sum
      }
    }

    private def norm[K](v: Map[K, Double]): Double =
      math.sqrt(dot(v, v))

    private def similarity[K](u: Map[K, Double], v: Map[K, Double]): Double =
      dot(u, v) / (norm(u) * norm(v))

    def hasHeader(chunk: String, rowDelim: String, separator: String, quote: String): Boolean = {
      def mkVec(s: String): Map[Char, Double] = {
        val scaled = s.groupBy(c => c).map { case (k, v) => k -> v.length.toDouble }
        val length = norm(scaled)
        scaled.map { case (k, v) => (k, v / length) }
      }

      val headerEnd = chunk.indexOf(rowDelim)
      if (headerEnd > 0) {
        val (hdr, rows) = chunk.replace(separator, "").replace(quote, "").splitAt(headerEnd)
        println(s"header = ${similarity(mkVec(hdr), mkVec(rows))}")
        similarity(mkVec(hdr), mkVec(rows)) < 0.5
      } else {
        false
      }
    }
  }
}
