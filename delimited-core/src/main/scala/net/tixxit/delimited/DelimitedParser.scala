package net.tixxit.delimited

import java.nio.charset.{ Charset, StandardCharsets }
import java.io.File
import java.io.{ InputStream, FileInputStream }
import java.io.{ Reader, InputStreamReader }

trait DelimitedParser {
  def format: Option[DelimitedFormat]

  def parseChunk(chunk: Option[String]): (DelimitedParser, Vector[Either[DelimitedError, Row]])

  def parseAll(chunks: Iterator[String]): Iterator[Either[DelimitedError, Row]] = {
    val input = chunks.map(Option(_)).takeWhile(_.isDefined) ++ Iterator(None)

    input
      .scanLeft((this, Vector.empty[Either[DelimitedError, Row]])) {
        case ((parser, _), chunk) => parser.parseChunk(chunk)
      }
      .flatMap(_._2)
  }

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

  def parseInputStream(is: InputStream, charset: Charset = StandardCharsets.UTF_8): Iterator[Either[DelimitedError, Row]] =
    parseReader(new InputStreamReader(is, charset))

  def parseFile(file: File, charset: Charset = StandardCharsets.UTF_8): Vector[Either[DelimitedError, Row]] = {
    val is = new FileInputStream(file)
    try {
      parseInputStream(is, charset).toVector
    } finally {
      is.close()
    }
  }

  def parseString(input: String): Vector[Either[DelimitedError, Row]] =
    parseAll(Iterator(input)).toVector
}

object DelimitedParser {
  val BufferSize = 64 * 1024

  def apply(): DelimitedParser = apply(DelimitedFormat.Guess)

  def apply(format: DelimitedFormatStrategy): DelimitedParser =
    parser.DelimitedParserImpl(format)
}
