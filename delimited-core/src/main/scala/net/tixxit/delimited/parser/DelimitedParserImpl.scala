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

final case class DelimitedParserImpl(
  strategy: DelimitedFormatStrategy,
  parserState: ParserState,
  fail: Option[Fail],
  row: Long,
  bufferSize: Int,
  maxCharsPerRow: Int
) extends DelimitedParser {
  require(maxCharsPerRow >= 0, "max row characters parameter must be non-negative")

  def format: Option[DelimitedFormat] = strategy match {
    case (fmt: DelimitedFormat) => Some(fmt)
    case _ => None
  }

  def reset: (String, DelimitedParserImpl) = {
    val in = parserState.input
    (in.substring(in.mark, in.limit), DelimitedParserImpl(strategy, bufferSize, maxCharsPerRow))
  }

  def parseChunk(chunk: Option[String]): (DelimitedParserImpl, Vector[Either[DelimitedError, Row]]) = {
    val initState = chunk match {
      case Some(str) => parserState.mapInput(_.append(str))
      case None => parserState.mapInput(_.finished)
    }
    val format = strategy match {
      case (guess: GuessDelimitedFormat) =>
        if (initState.input.isLast || initState.input.data.length >= bufferSize) {
          // We want <hand-waving>enough</hand-waving> data here.
          guess(initState.input.data)
        } else {
          // TODO: We could get rid of this return.
          return (DelimitedParserImpl(strategy, initState, fail, row, bufferSize, maxCharsPerRow), Vector.empty)
        }
      case (fmt: DelimitedFormat) =>
        fmt
    }

    val maxRowDelimLength: Int = format.rowDelim match {
      case RowDelim(value, None) => value.length
      case RowDelim(value, Some(alt)) => scala.math.max(value.length, alt.length)
    }

    @tailrec
    def loop(s0: ParserState, fail: Option[Fail], row: Long, acc: Vector[Either[DelimitedError, Row]]): (DelimitedParserImpl, Vector[Either[DelimitedError, Row]]) = {
      val (s1, instr) = DelimitedParserImpl.parse(format)(s0)

      instr match {
        case EmitRow(cells) =>
          if (maxCharsPerRow > 0 && (s1.rowStart - s0.rowStart) > maxCharsPerRow) {
              val context = DelimitedParserImpl.removeRowDelim(format,
                s1.input.substring(s0.rowStart, s1.rowStart))
            val error = DelimitedError(s"row exceeded maximum length of $maxCharsPerRow",
              s0.rowStart, s0.rowStart, context, row, 1)
            loop(s1, fail, row + 1, acc :+ Left(error))
          } else {
            loop(s1, fail, row + 1, acc :+ Right(cells))
          }

        case f @ Fail(_, _) =>
          loop(s1, Some(f), row, acc)

        case Resume =>
          fail match {
            case Some(Fail(msg, pos)) =>
              val context = DelimitedParserImpl.removeRowDelim(format, s1.input.substring(s0.rowStart, s1.rowStart))
              val error = DelimitedError(msg, s0.rowStart, pos, context, row, pos - s0.rowStart + 1)
              loop(s1, None, row + 1, acc :+ Left(error))

            case None =>
              loop(s1, None, row, acc)
          }

        case NeedInput =>
          if (maxCharsPerRow > 0 && fail.isEmpty &&
              (s1.input.limit - s1.rowStart - maxRowDelimLength) > maxCharsPerRow) {
            val f = Some(Fail(s"row exceeded maximum length of $maxCharsPerRow", s1.rowStart))
            DelimitedParserImpl(format, s1.skipRow, f, row, bufferSize, maxCharsPerRow) -> acc
          } else {
            DelimitedParserImpl(format, s1, fail, row, bufferSize, maxCharsPerRow) -> acc
          }

        case Done =>
          DelimitedParserImpl(format, s1, None, row, bufferSize, maxCharsPerRow) -> acc
      }
    }

    loop(initState, fail, row, Vector.empty)
  }
}

object DelimitedParserImpl {

  /**
   * Returns a new DelimitedParserImpl whose state is initially empty.
   *
   * @note If `maxRowsChars` is 0, then there is no limit on row size.
   *
   * @param format the format strategy to use for parsing
   * @param bufferSize the minimum size of the buffer to use for format inference
   * @param maxRowsChars a hard limit on the allowable size of a row
   */
  def apply(
    format: DelimitedFormatStrategy,
    bufferSize: Int,
    maxCharsPerRow: Int
  ): DelimitedParserImpl = {
    DelimitedParserImpl(format, ParserState.ParseRow(0L, 0L, Input.init("")), None, 1L, bufferSize, maxCharsPerRow)
  }

  def parse(format: DelimitedFormat)(state: ParserState): (ParserState, Instr) = {
    import format._

    val input: Input = state.input
    val buf: InputBuffer = new InputBuffer(state)

    def isQuote(): Int = buf.isFlag(quote)
    def isQuoteEscape(): Int = buf.isFlag(quoteEscape)
    def isSeparator(): Int = buf.isFlag(separator)

    val primaryRowDelim: String = rowDelim.value
    val secondaryRowDelim: String = rowDelim.alternate.orNull
    def isRowDelim(): Int = buf.eitherFlag(primaryRowDelim, secondaryRowDelim)
    def isEndOfCell(): Int = {
      val i = isSeparator()
      if (i == 0) isRowDelim() else i
    }

    def isEscapedQuote(): Int = {
      val e = isQuoteEscape()
      if (e > 0) {
        buf.advance(e)
        val q = isQuote()
        buf.retreat(e)
        if (q > 0) q + e
        else q
      } else {
        e
      }
    }

    def unquotedCell(bldr: Builder[String, Row]): ParseResult = {
      val start = buf.getPos()
      def loop(): ParseResult = {
        val flag = isEndOfCell()
        if (flag > 0 || buf.endOfFile()) {
          val value = input.substring(start, buf.getPos())
          bldr += value
          Success
        } else if (flag == 0) {
          buf.advance(1)
          loop()
        } else {
          NeedInput
        }
      }

      loop()
    }

    def quotedCell(bldr: Builder[String, Row]): ParseResult = {
      val start = buf.getPos()
      def loop(): ParseResult = {
        if (buf.endOfInput()) {
          if (buf.endOfFile()) {
            Fail("Unmatched quoted string at end of file", buf.getPos())
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
            Fail("Unmatched quoted string at row delimiter", buf.getPos())
          } else if (e > 0) {
            buf.advance(e)
            loop()
          } else if (q > 0) {
            val unescaped = unescape(input.substring(start, buf.getPos()))
            buf.advance(q)
            bldr += unescaped
            Success
          } else {
            buf.advance(1)
            loop()
          }
        }
      }

      loop()
    }

    def cell(bldr: Builder[String, Row]): ParseResult = {
      val q = isQuote()
      if (q == 0) {
        unquotedCell(bldr)
      } else if (q > 0) {
        buf.advance(q)
        quotedCell(bldr)
      } else {
        NeedInput
      }
    }

    def skipToNextRow(): Boolean = {
      val d = isRowDelim()
      if (d > 0 || buf.endOfFile()) {
        buf.advance(d)
        true
      } else if (d == 0) {
        buf.advance(1)
        skipToNextRow()
      } else {
        if (input.isLast)
          buf.advance((input.limit - buf.getPos()).toInt)
        input.isLast
      }
    }

    def row(rowStart: Long, cells: Builder[String, Row]): (ParserState, Instr) = {
      val start = buf.getPos()
      def needInput() = (ContinueRow(rowStart, start, cells.result(), input), NeedInput)

      val s = isSeparator()
      if (s == 0) {
        val r = isRowDelim()
        if (r > 0 || buf.endOfFile()) {
          buf.advance(r)
          val row = cells.result()
          (ParseRow(buf.getPos(), buf.getPos(), input.marked(buf.getPos()), row.size), EmitRow(row))
        } else if (r == 0) {
          (SkipRow(rowStart, buf.getPos(), input), Fail("Expected separator, row delimiter, or end of file", buf.getPos()))
        } else {
          needInput()
        }
      } else if (s > 0) {
        buf.advance(s)
        cell(cells) match {
          case Success =>
            row(rowStart, cells)
          case f @ Fail(_, _) =>
            (SkipRow(rowStart, buf.getPos(), input), f)
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
        if (buf.endOfFile()) {
          (instr, Done)
        } else {
          val cells = state.newRowBuilder
          cell(cells) match {
            case Success =>
              row(rowStart, cells)
            case f @ Fail(_, _) =>
              (SkipRow(rowStart, buf.getPos(), input, sizeHint), f)
            case NeedInput =>
              (instr, NeedInput)
          }
        }

      case SkipRow(rowStart, readFrom, _, sizeHint) =>
        if (skipToNextRow()) {
          (ParseRow(buf.getPos(), buf.getPos(), input.marked(buf.getPos()), sizeHint), Resume)
        } else {
          (SkipRow(rowStart, buf.getPos(), input, sizeHint), NeedInput)
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
