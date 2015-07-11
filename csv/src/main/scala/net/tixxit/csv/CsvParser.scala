package net.tixxit.csv

import scala.annotation.tailrec

import java.nio.charset.{ Charset, StandardCharsets }
import java.io.File
import java.io.{ InputStream, FileInputStream }
import java.io.{ Reader, InputStreamReader }

import ParserState._
import Instr._

case class CsvParser(
  strategy: CsvFormatStrategy,
  parserState: ParserState,
  fail: Option[Fail],
  row: Long
) {
  def parseChunk(chunk: Option[String]): (CsvParser, Vector[Either[CsvError, CsvRow]]) = {
    val initState = chunk match {
      case Some(str) => parserState.mapInput(_.append(str))
      case None => parserState.mapInput(_.finished)
    }
    val format = strategy match {
      case (guess: GuessCsvFormat) =>
        if (initState.input.isLast || initState.input.data.length > CsvParser.BufferSize / 2) {
          // We want <hand-waving>enough</hand-waving> data here, so say 1/2 the buffer size?
          guess(initState.input.data)
        } else {
          // TODO: We could get rid of this return.
          return (CsvParser(strategy, initState, fail, row), Vector.empty)
        }
      case (fmt: CsvFormat) =>
        fmt
    }

    @tailrec
    def loop(s0: ParserState, fail: Option[Fail], row: Long, acc: Vector[Either[CsvError, CsvRow]]): (CsvParser, Vector[Either[CsvError, CsvRow]]) = {
      val (s1, instr) = CsvParser.parse(format)(s0)

      instr match {
        case Emit(cells) =>
          loop(s1, fail, row + 1, acc :+ Right(cells))

        case f @ Fail(_, _) =>
          loop(s1, Some(f), row, acc)

        case Resume =>
          fail match {
            case Some(Fail(msg, pos)) =>
              val context = CsvParser.removeRowDelim(format, s1.input.substring(s0.rowStart, s1.rowStart))
              val error = CsvError(msg, s0.rowStart, pos, context, row, pos - s0.rowStart + 1)
              loop(s1, None, row + 1, acc :+ Left(error))

            case None =>
              loop(s1, None, row, acc)
          }

        case NeedInput =>
          CsvParser(format, s1, fail, row) -> acc

        case Done =>
          CsvParser(format, s1, None, row) -> acc
      }
    }

    loop(initState, fail, row, Vector.empty)
  }

  def parseAll(chunks: Iterator[String]): Vector[Either[CsvError, CsvRow]] = {
    val input = chunks.map(Option(_)).takeWhile(_.isDefined) ++ Iterator(None)
    input.foldLeft((this, Vector.empty[Either[CsvError, CsvRow]])) {
      case ((parser, prefix), chunk) =>
      val (nextParser, rows) = parser.parseChunk(chunk)
      (nextParser, prefix ++ rows)
    }._2
  }

  def parseReader(reader: Reader): Vector[Either[CsvError, CsvRow]] = {
    val buffer = new Array[Char](CsvParser.BufferSize)
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

  def parseInputStream(is: InputStream, charset: Charset = StandardCharsets.UTF_8): Vector[Either[CsvError, CsvRow]] =
    parseReader(new InputStreamReader(is, charset))

  def parseFile(file: File, charset: Charset = StandardCharsets.UTF_8): Vector[Either[CsvError, CsvRow]] = {
    val is = new FileInputStream(file)
    try {
      parseInputStream(is, charset)
    } finally {
      is.close()
    }
  }

  def parseString(input: String): Vector[Either[CsvError, CsvRow]] =
    parseAll(Iterator(input))
}

object CsvParser {
  val BufferSize = 32 * 1024

  def apply(format: CsvFormat): CsvParser =
    CsvParser(format, ParserState.ParseRow(0L, 0L, Input.init("")), None, 1L)

  def parse(format: CsvFormat)(state: ParserState): (ParserState, Instr[CsvRow]) = {
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

    def unquotedCell(): ParseResult[CsvCell] = {
      val start = pos
      def loop(): ParseResult[CsvCell] = {
        val flag = isEndOfCell()
        if (flag > 0 || endOfFile) {
          val value = input.substring(start, pos)
          val csvCell =
            if (value == empty) CsvCell.Empty
            else if (value == invalid) CsvCell.Invalid
            else CsvCell.Data(value)
          Emit(csvCell)
        } else if (flag == 0) {
          advance()
          loop()
        } else {
          NeedInput
        }
      }

      loop()
    }

    def quotedCell(): ParseResult[CsvCell] = {
      val start = pos
      def loop(): ParseResult[CsvCell] = {
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
            val escaped = input.substring(start, pos).replace(escapedQuote, quote)
            advance(q)
            Emit(CsvCell.Data(escaped))
          } else {
            advance(1)
            loop()
          }
        }
      }

      loop()
    }

    def cell(): ParseResult[CsvCell] = {
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

    def row(rowStart: Long, cells: Vector[CsvCell]): (ParserState, Instr[CsvRow]) = {
      val start = pos
      def needInput() = (ContinueRow(rowStart, start, cells, input), NeedInput)

      val s = isSeparator()
      if (s == 0) {
        val r = isRowDelim()
        if (r > 0 || endOfFile) {
          advance(r)
          (ParseRow(pos, pos, input.marked(pos)), Emit(new CsvRow(cells)))
        } else if (r == 0) {
          (SkipRow(rowStart, pos, input), Fail("Expected separator, row delimiter, or end of file", pos))
        } else {
          needInput()
        }
      } else if (s > 0) {
        advance(s)
        cell() match {
          case Emit(c) =>
            row(rowStart, cells :+ c)
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
      case ContinueRow(rowStart, readFrom, partial, _) =>
        row(rowStart, partial)

      case instr @ ParseRow(rowStart, readFrom, _) =>
        if (endOfFile) {
          (instr, Done)
        } else {
          cell() match {
            case Emit(csvCell) =>
              row(rowStart, Vector(csvCell))
            case f @ Fail(_, _) =>
              (SkipRow(rowStart, pos, input), f)
            case NeedInput =>
              (instr, NeedInput)
          }
        }

      case SkipRow(rowStart, readFrom, _) =>
        if (skipToNextRow()) {
          (ParseRow(pos, pos, input.marked(pos)), Resume)
        } else {
          (SkipRow(rowStart, pos, input), NeedInput)
        }
    }
  }

  private def removeRowDelim(format: CsvFormat, context: String): String = {
    def dropTail(tail: String): Option[String] =
      if (context.endsWith(tail)) Some(context.dropRight(tail.length))
      else None

    dropTail(format.rowDelim.value).
      orElse(format.rowDelim.alternate.flatMap(dropTail)).
      getOrElse(context)
  }
}
