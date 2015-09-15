package net.tixxit.delimited
package parser

import scala.annotation.tailrec
import scala.collection.mutable.Builder

import java.nio.charset.{ Charset, StandardCharsets }
import java.io.File
import java.io.{ InputStream, FileInputStream }
import java.io.{ Reader, InputStreamReader }

import ParserState._
import Instr._

case class DelimitedParser(
  strategy: DelimitedFormatStrategy,
  parserState: ParserState,
  fail: Option[Fail],
  row: Long
) {
  def parseChunk(chunk: Option[String]): (DelimitedParser, Vector[Either[DelimitedError, Row]]) = {
    val initState = chunk match {
      case Some(str) => parserState.mapInput(_.append(str))
      case None => parserState.mapInput(_.finished)
    }
    val format = strategy match {
      case (guess: GuessDelimitedFormat) =>
        if (initState.input.isLast || initState.input.data.length > DelimitedParser.BufferSize / 2) {
          // We want <hand-waving>enough</hand-waving> data here, so say 1/2 the buffer size?
          guess(initState.input.data)
        } else {
          // TODO: We could get rid of this return.
          return (DelimitedParser(strategy, initState, fail, row), Vector.empty)
        }
      case (fmt: DelimitedFormat) =>
        fmt
    }

    @tailrec
    def loop(s0: ParserState, fail: Option[Fail], row: Long, acc: Vector[Either[DelimitedError, Row]]): (DelimitedParser, Vector[Either[DelimitedError, Row]]) = {
      val (s1, instr) = DelimitedParser.parse(format)(s0)

      instr match {
        case Emit(cells) =>
          loop(s1, fail, row + 1, acc :+ Right(cells))

        case f @ Fail(_, _) =>
          loop(s1, Some(f), row, acc)

        case Resume =>
          fail match {
            case Some(Fail(msg, pos)) =>
              val context = DelimitedParser.removeRowDelim(format, s1.input.substring(s0.rowStart, s1.rowStart))
              val error = DelimitedError(msg, s0.rowStart, pos, context, row, pos - s0.rowStart + 1)
              loop(s1, None, row + 1, acc :+ Left(error))

            case None =>
              loop(s1, None, row, acc)
          }

        case NeedInput =>
          DelimitedParser(format, s1, fail, row) -> acc

        case Done =>
          DelimitedParser(format, s1, None, row) -> acc
      }
    }

    loop(initState, fail, row, Vector.empty)
  }

  def parseAll(chunks: Iterator[String]): Vector[Either[DelimitedError, Row]] = {
    val input = chunks.map(Option(_)).takeWhile(_.isDefined) ++ Iterator(None)
    input.foldLeft((this, Vector.empty[Either[DelimitedError, Row]])) {
      case ((parser, prefix), chunk) =>
      val (nextParser, rows) = parser.parseChunk(chunk)
      (nextParser, prefix ++ rows)
    }._2
  }

  def parseReader(reader: Reader): Vector[Either[DelimitedError, Row]] = {
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

  def parseInputStream(is: InputStream, charset: Charset = StandardCharsets.UTF_8): Vector[Either[DelimitedError, Row]] =
    parseReader(new InputStreamReader(is, charset))

  def parseFile(file: File, charset: Charset = StandardCharsets.UTF_8): Vector[Either[DelimitedError, Row]] = {
    val is = new FileInputStream(file)
    try {
      parseInputStream(is, charset)
    } finally {
      is.close()
    }
  }

  def parseString(input: String): Vector[Either[DelimitedError, Row]] =
    parseAll(Iterator(input))
}

object DelimitedParser {
  val BufferSize = 32 * 1024

  def apply(format: DelimitedFormat): DelimitedParser =
    DelimitedParser(format, ParserState.ParseRow(0L, 0L, Input.init("")), None, 1L)

  def parse(format: DelimitedFormat)(state: ParserState): (ParserState, Instr[Row]) = {
    import format._

    val input: Input = state.input
    var pos: Long = state.readFrom
    def ch: Char = input.charAt(pos)
    def endOfInput: Boolean = pos >= input.length
    def endOfFile: Boolean = endOfInput && input.isLast
    def advance(i: Long = 1): Unit = pos += i
    def retreat(i: Long = 1): Unit = pos -= i

    def isFlag(str: String): () => Int = {
      def loop(i: Int): Int =
        if (i >= str.length) {
          retreat(i)
          i
        } else if (endOfInput) {
          retreat(i)
          if (endOfFile) 0 else -1
        } else if (str.charAt(i) == ch) {
          advance()
          loop(i + 1)
        } else {
          retreat(i)
          0
        }

      () => loop(0)
    }

    def either(f0: () => Int, f1: () => Int): () => Int = { () =>
      val i = f0()
      if (i == 0) f1() else i
    }

    val isQuote = isFlag(quote)
    val isQuoteEscape = isFlag(quoteEscape)
    val isSeparator = isFlag(separator)
    val isRowDelim = rowDelim.alternate.map { alt =>
      either(isFlag(rowDelim.value), isFlag(alt))
    }.getOrElse(isFlag(rowDelim.value))
    val isEndOfCell = either(isSeparator, isRowDelim)
    def isEscapedQuote() = {
      val e = isQuoteEscape()
      if (e > 0) {
        advance(e)
        val q = isQuote()
        retreat(e)
        if (q > 0) q + e
        else q
      } else {
        e
      }
    }

    def unquotedCell(): ParseResult[String] = {
      val start = pos
      def loop(): ParseResult[String] = {
        val flag = isEndOfCell()
        if (flag > 0 || endOfFile) {
          val value = input.substring(start, pos)
          Emit(value)
        } else if (flag == 0) {
          advance()
          loop()
        } else {
          NeedInput
        }
      }

      loop()
    }

    def quotedCell(): ParseResult[String] = {
      val start = pos
      def loop(): ParseResult[String] = {
        if (endOfInput) {
          if (endOfFile) {
            Fail("Unmatched quoted string at end of file", pos)
          } else {
            NeedInput
          }
        } else {
          val d = if (allowRowDelimInQuotes) 0 else isRowDelim()
          val e = isEscapedQuote()
          val q = isQuote()

          if (d < 0 || e < 0 || q < 0) {
            NeedInput
          } else if (d > 0) {
            Fail("Unmatched quoted string at row delimiter", pos)
          } else if (e > 0) {
            advance(e)
            loop()
          } else if (q > 0) {
            val unescaped = unescape(input.substring(start, pos))
            advance(q)
            Emit(unescaped)
          } else {
            advance(1)
            loop()
          }
        }
      }

      loop()
    }

    def cell(): ParseResult[String] = {
      val q = isQuote()
      if (q == 0) {
        unquotedCell()
      } else if (q > 0) {
        advance(q)
        quotedCell()
      } else {
        NeedInput
      }
    }

    def skipToNextRow(): Boolean = {
      val d = isRowDelim()
      if (d > 0 || endOfFile) {
        advance(d)
        true
      } else if (d == 0) {
        advance(1)
        skipToNextRow()
      } else {
        if (input.isLast)
          advance(input.length - pos)
        input.isLast
      }
    }

    def row(rowStart: Long, cells: Builder[String, Row]): (ParserState, Instr[Row]) = {
      val start = pos
      def needInput() = (ContinueRow(rowStart, start, cells.result(), input), NeedInput)

      val s = isSeparator()
      if (s == 0) {
        val r = isRowDelim()
        if (r > 0 || endOfFile) {
          advance(r)
          val row = cells.result()
          (ParseRow(pos, pos, input.marked(pos), row.size), Emit(row))
        } else if (r == 0) {
          (SkipRow(rowStart, pos, input), Fail("Expected separator, row delimiter, or end of file", pos))
        } else {
          needInput()
        }
      } else if (s > 0) {
        advance(s)
        cell() match {
          case Emit(c) =>
            row(rowStart, cells += c)
          case f @ Fail(_, _) =>
            (SkipRow(rowStart, pos, input), f)
          case NeedInput =>
            needInput()
        }
      } else {
        needInput()
      }
    }

    state match {
      case ContinueRow(rowStart, readFrom, partial, _, _) =>
        row(rowStart, state.newRowBuilder ++= partial)

      case instr @ ParseRow(rowStart, readFrom, _, sizeHint) =>
        if (endOfFile) {
          (instr, Done)
        } else {
          cell() match {
            case Emit(csvCell) =>
              row(rowStart, state.newRowBuilder += csvCell)
            case f @ Fail(_, _) =>
              (SkipRow(rowStart, pos, input, sizeHint), f)
            case NeedInput =>
              (instr, NeedInput)
          }
        }

      case SkipRow(rowStart, readFrom, _, sizeHint) =>
        if (skipToNextRow()) {
          (ParseRow(pos, pos, input.marked(pos), sizeHint), Resume)
        } else {
          (SkipRow(rowStart, pos, input, sizeHint), NeedInput)
        }
    }
  }

  private def removeRowDelim(format: DelimitedFormat, context: String): String = {
    def dropTail(tail: String): Option[String] =
      if (context.endsWith(tail)) Some(context.dropRight(tail.length))
      else None

    dropTail(format.rowDelim.value).
      orElse(format.rowDelim.alternate.flatMap(dropTail)).
      getOrElse(context)
  }
}
