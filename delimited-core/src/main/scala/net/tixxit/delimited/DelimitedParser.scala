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
    val input = chunks.takeWhile(_ ne null).map(Some(_)) ++ Iterator.single(None)

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
    parseAll(Iterator.single(input)).toVector
}

object DelimitedParser {
  val BufferSize = 64 * 1024

  /**
   * Returns a `DelimitedParser` that can parse delimited files using the
   * strategy or format provided.
   *
   * @param format         strategy used to determine the format
   * @param bufferSize     minimum size of buffer used for format inference
   * @param maxCharsPerRow hard limit on the # of chars in a row, or 0 if there
   *                       is no limit
   */
  def apply(
    format: DelimitedFormatStrategy,
    bufferSize: Int = BufferSize,
    maxCharsPerRow: Int = 0
  ): DelimitedParser = {
    parser.DelimitedParserImpl(format, bufferSize, maxCharsPerRow)
  }

  /**
   * Parses a single row of a delimited file. This usually shouldn't be used to
   * parse a file, but it is useful in constrained environments where the rows
   * are pre-parsed already and all the state management of a `DelimitedParser`
   * instance isn't needed.
   *
   * This method expects exactly 1 row. This row may have the row delimiter at
   * the end. If a row is successfully parsed, but more text input remains,
   * then this will return an error.
   *
   * For example, many big data systems (eg Cascading, Scalding) has text-line
   * based input formats, upon which TSV/CSV parsing can be built. In these
   * cases, the rows are supplied to the mapper, so we don't need any state
   * management.
   *
   * @param format the format used to parse the row
   * @param row    the text row, without any row-delimiters
   */
  def parseRow(format: DelimitedFormat, row: String): Either[DelimitedError, Row] =
    parser.DelimitedParserImpl.parseRow(format, row)
}
