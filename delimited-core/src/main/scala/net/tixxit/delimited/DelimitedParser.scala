package net.tixxit.delimited

import java.nio.charset.{ Charset, StandardCharsets }
import java.io.File
import java.io.{ InputStream, FileInputStream }
import java.io.{ Reader, InputStreamReader }

/**
 * An immutable parser for delimited files. This operates on chunks of input,
 * using the `parseChunk` method. After parsing a chunk, the `parseChunk` method
 * returns a new `DelimitedParser` as well as all of the complete rows parsed
 * in that chunk. Any partially complete rows will be returned in a future call
 * to `parseChunk` in either the returned `DelimitedParser` or a future one in
 * a chain of calls to `parseChunk`.
 *
 * There are also convenience methods for parsing `File`s, `String`s,
 * `InputStream`s, `Reader`s, etc.
 *
 * To get an instance of a `DelimitedParser` that can be used to parse a CSV,
 * TSV, etc file, you can use something like:
 *
 * {{{
 * val parser = DelimitedParser(DelimitedFormat.CSV)
 * val rows: Vector[Either[DelimitedError, Row]] =
 *   parser.parseFile(new java.io.File("some.csv"))
 * }}}
 *
 * If you don't know the format of your delimited file ahead of time, not much
 * changes:
 *
 * {{{
 * val parser = DelimitedParser(DelimitedFormat.Guess)
 * val rows: Vector[Either[DelimitedError, Row]] =
 *   parser.parseFile(new java.io.File("some.csv"))
 * }}}
 */
trait DelimitedParser {

  /**
   * The [[DelimitedFormat]] being used to parse this delimited file, or `None`
   * if a format has not yet been inferred (in which case, no rows have yet
   * been returned by `parseChunk`).
   */
  def format: Option[DelimitedFormat]

  /**
   * Parse a chunk of the input if there is any left. If chunk is `None`, then
   * that indicates to the parser that there will be no further input. In this
   * case (`chunk` is `None`), all remaining input will be consumed and
   * returned as rows (or errors).
   *
   * This returns a new `DelimitedParser` to use to parse the next chunk, as
   * well as a `Vector` of all complete rows parsed from `chunk`.
   *
   * @param chunk the next chunk of data as a String, or None if eof
   */
  def parseChunk(chunk: Option[String]): (DelimitedParser, Vector[Either[DelimitedError, Row]])

  /**
   * Returns all unparsed data and a DelimitedParser whose state is completely
   * reset.
   */
  def reset: (String, DelimitedParser)

  /**
   * Parse all chunks in the given iterator, consecutively, treating the last
   * chunk in `chunks` as the final input. This will return all rows from the
   * input.
   */
  def parseAll(chunks: Iterator[String]): Iterator[Either[DelimitedError, Row]] = {
    val input = chunks.map(Option(_)).takeWhile(_.isDefined) ++ Iterator(None)

    input
      .scanLeft((this, Vector.empty[Either[DelimitedError, Row]])) {
        case ((parser, _), chunk) => parser.parseChunk(chunk)
      }
      .flatMap(_._2)
  }

  /**
   * Returns an iterator that parses rows from `reader` as elements are
   * consumed.
   */
  def parseReader(reader: Reader): Iterator[Either[DelimitedError, Row]] = {
    val buffer = new Array[Char](DelimitedParser.BufferSize)
    val chunks = Iterator.continually {
      val len = reader.read(buffer)
      if (len >= 0) {
        new String(buffer, 0, len)
      } else {
        null
      }
    }.takeWhile(_ != null)

    parseAll(chunks)
  }

  /**
   * Returns an iterator that parses rows from `in` as elements are
   * consumed.
   *
   * @param in      the raw input as bytes
   * @param charset the character set to decode the bytes as
   */
  def parseInputStream(is: InputStream, charset: Charset = StandardCharsets.UTF_8): Iterator[Either[DelimitedError, Row]] =
    parseReader(new InputStreamReader(is, charset))

  /**
   * Completely parses `file` and returns all the rows in a `Vector`.
   *
   * @param file    the TSV file on disk
   * @param charset the character set the TSV was encoded in
   */
  def parseFile(file: File, charset: Charset = StandardCharsets.UTF_8): Vector[Either[DelimitedError, Row]] = {
    val is = new FileInputStream(file)
    try {
      parseInputStream(is, charset).toVector
    } finally {
      is.close()
    }
  }

  /**
   * Parses an entire delimited file as a string.
   */
  def parseString(input: String): Vector[Either[DelimitedError, Row]] =
    parseAll(Iterator(input)).toVector
}

object DelimitedParser {
  val BufferSize = 64 * 1024

  /**
   * Returns a `DelimitedParser` that can parse delimited files using the
   * strategy or format provided.
   */
  def apply(
    format: DelimitedFormatStrategy,
    bufferSize: Int = BufferSize
  ): DelimitedParser = {
    parser.DelimitedParserImpl(format, bufferSize)
  }
}
