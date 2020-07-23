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


    def setSizeHint(ps: ParserState, sizeHint: Int): ParserState =
      if (ps.sizeHint >= sizeHint) ps
      else
        ps match {
          case c@ContinueRow(_, _, _, _, _) => c.copy(sizeHint = sizeHint)
          case s@SkipRow(_, _, _, _) => s.copy(sizeHint = sizeHint)
          case p@ParseRow(_, _, _, _) => p.copy(sizeHint = sizeHint)
        }

    val bldr = Vector.newBuilder[Either[DelimitedError, Row]]

    @tailrec
    def loop(s0: ParserState, fail: Option[Fail], row: Long): DelimitedParserImpl = {
      val (s1, instr) = DelimitedParserImpl.parse(format)(s0)

      instr match {
        case EmitRow(cells) =>
          if (maxCharsPerRow > 0 && (s1.rowStart - s0.rowStart - maxRowDelimLength) > maxCharsPerRow) {
              val context = DelimitedParserImpl.removeRowDelim(format,
                s1.input.substring(s0.rowStart, s1.rowStart))
            val error = DelimitedError(s"row exceeded maximum length of $maxCharsPerRow",
              s0.rowStart, s0.rowStart, context, row, 1)
            bldr += Left(error)
          } else {
            bldr += Right(cells)
          }
          val s2 = setSizeHint(s1, cells.length)
          loop(s2, fail, row + 1)

        case f @ Fail(_, _) =>
          loop(s1, Some(f), row)

        case Resume =>
          val nextRow =
            if (fail.isDefined) {
              val f = fail.get
              val pos = f.pos
              val context = DelimitedParserImpl.removeRowDelim(format, s1.input.substring(s0.rowStart, s1.rowStart))
              val error = DelimitedError(f.message, s0.rowStart, pos, context, row, pos - s0.rowStart + 1)
              bldr += Left(error)
              row + 1L
            }
            else row

          loop(s1, None, nextRow)

        case NeedInput =>
          if (maxCharsPerRow > 0 && fail.isEmpty &&
              (s1.input.limit - s1.rowStart - maxRowDelimLength) > maxCharsPerRow) {
            val f = Some(Fail(s"row exceeded maximum length of $maxCharsPerRow", s1.rowStart))
            DelimitedParserImpl(format, s1.skipRow, f, row, bufferSize, maxCharsPerRow)
          } else {
            DelimitedParserImpl(format, s1, fail, row, bufferSize, maxCharsPerRow)
          }

        case Done =>
          DelimitedParserImpl(format, s1, None, row, bufferSize, maxCharsPerRow)
      }
    }

    val nextImpl = loop(initState, fail, row)
    (nextImpl, bldr.result())
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

  def parseRow(format: DelimitedFormat, row: String): Either[DelimitedError, Row] = {
    val s0 = ParserState.ParseRow(0L, 0L, Input.last(row))
    DelimitedParserImpl.parse(format)(s0) match {
      case (s, Instr.EmitRow(row)) if s.rowStart == s.input.data.length =>
        Right(row)
      case (s, Instr.EmitRow(_)) =>
        Left(DelimitedError("unexpected start of new row",
                            0, s.rowStart, row, 1, s.rowStart + 1))
      case (_, Instr.Fail(message, pos)) =>
        Left(DelimitedError(message, 0, pos, row, 1, pos + 1))
      case (_, Instr.NeedInput | Instr.Resume | Instr.Done) =>
        Left(DelimitedError("empty row", 0, 0, row, 1, 1))
    }
  }

  def parse(format: DelimitedFormat)(state: ParserState): (ParserState, Instr) = {
    import format._

    val input: Input = state.input
    val buf: InputBuffer = new InputBuffer(state)

    def isQuote(): Int = buf.isFlag(quote)
    def isQuoteEscape(): Int = buf.isFlag(quoteEscape)
    @inline def isSeparator(): Int = buf.isFlag(separator)

    val primaryRowDelim: String = rowDelim.value
    val secondaryRowDelim: String = rowDelim.alternate.orNull

    @inline def isRowDelim(): Int = buf.eitherFlag(primaryRowDelim, secondaryRowDelim)
    @inline def isEndOfCell(): Int = {
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
      var res: ParseResult = null
      buf.advanceGT(maxSep)
      while (res eq null) {
        val flag = isEndOfCell()
        if (flag > 0 || buf.endOfFile()) {
          val value = input.substring(start, buf.getPos())
          bldr += value
          res = Success
        } else if (flag == 0) {
          buf.advance(1)
        } else {
          res = NeedInput
        }
      }

      res
    }

    def quotedCell(bldr: Builder[String, Row]): ParseResult = {
      val start = buf.getPos()
      var res: ParseResult = null
      while (res eq null) {
        if (buf.endOfInput()) {
          if (buf.endOfFile()) {
            res = Fail("Unmatched quoted string at end of file", buf.getPos())
          } else {
            res = NeedInput
          }
        } else {
          val d = if (allowRowDelimInQuotes) 0 else isRowDelim()
          val e = isEscapedQuote()
          val q = isQuote()

          if (d < 0 || e < 0 || q < 0) {
            res = NeedInput
          } else if (d > 0) {
            res = Fail("Unmatched quoted string at row delimiter", buf.getPos())
          } else if (e > 0) {
            buf.advance(e)
          } else if (q > 0) {
            val unescaped = unescape(input.substring(start, buf.getPos()))
            buf.advance(q)
            bldr += unescaped
            res = Success
          } else {
            buf.advance(1)
          }
        }
      }

      res
    }

    @inline
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
      @inline def needInput() = (ContinueRow(rowStart, start, cells.result(), input), NeedInput)

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
      case cr: ContinueRow =>
        row(cr.rowStart, cr.partial.appendAllTo(cr.newRowBuilder))

      case instr: ParseRow =>
        if (buf.endOfFile()) {
          (instr, Done)
        } else {
          val cells = instr.newRowBuilder
          cell(cells) match {
            case Success =>
              row(instr.rowStart, cells)
            case f @ Fail(_, _) =>
              (SkipRow(instr.rowStart, buf.getPos(), input, instr.sizeHint), f)
            case NeedInput =>
              (instr, NeedInput)
          }
        }

      case sr: SkipRow =>
        if (skipToNextRow()) {
          (ParseRow(buf.getPos(), buf.getPos(), input.marked(buf.getPos()), sr.sizeHint), Resume)
        } else {
          (SkipRow(sr.rowStart, buf.getPos(), input, sr.sizeHint), NeedInput)
        }
    }
  }

  private def removeRowDelim(format: DelimitedFormat, context: String): String = {
    def dropTail(tail: String): String /* | Null */ =
      if (context.endsWith(tail)) context.dropRight(tail.length)
      else null

    val d0 = dropTail(format.rowDelim.value)
    if (d0 eq null) {
      format.rowDelim.alternate match {
        case Some(alt) =>
          val d1 = dropTail(alt)
          if (d1 eq null) context
          else d1

        case None => context
      }
    }
    else d0
  }
}
