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
        case EmitRow(cells) =>
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
  val BufferSize = 32 * 1024

  def apply(format: DelimitedFormatStrategy): DelimitedParser =
    DelimitedParser(format, ParserState.ParseRow(0L, 0L, Input.init("")), None, 1L)

  final class InputBuffer(input: Input) {
    def this(state: ParserState) = {
      this(state.input)
      setPos(state.readFrom)
    }

    private[this] var pos: Int = 0
    private[this] val chunk: String = input.data

    def getPos(): Long = input.offset + pos.toLong
    def setPos(p: Long): Unit = pos = (p.toLong - input.offset).toInt

    def getChar(): Char = chunk.charAt(pos)
    def advance(i: Int): Unit = pos += i
    def retreat(i: Int): Unit = pos -= i
    def endOfInput(): Boolean = pos >= input.length
    def endOfFile(): Boolean = endOfInput() && input.isLast

    def isFlag(str: String): Int = {
      def loop(i: Int): Int =
        if (i >= str.length) {
          retreat(i)
          i
        } else if (endOfInput()) {
          retreat(i)
          if (endOfFile) 0 else -1
        } else if (str.charAt(i) == getChar()) {
          advance(1)
          loop(i + 1)
        } else {
          retreat(i)
          0
        }

      loop(0)
    }

    def eitherFlag(f1: String, f2: String): Int = {
      val i = isFlag(f1)
      if (i == 0 && f2 != null) isFlag(f2) else i
    }
  }

  def parse(format: DelimitedFormat)(state: ParserState): (ParserState, Instr) = {
    import format._

    val input: Input = state.input
    val buf: InputBuffer = new InputBuffer(state)

    def either(f0: () => Int, f1: () => Int): () => Int = { () =>
      val i = f0()
      if (i == 0) f1() else i
    }

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
          buf.advance((input.length - buf.getPos()).toInt)
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
