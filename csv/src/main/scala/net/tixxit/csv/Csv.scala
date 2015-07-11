package net.tixxit.csv

import java.nio.charset.{ Charset, StandardCharsets }
import java.io.File
import java.io.{ InputStream, FileInputStream }
import java.io.{ Reader, InputStreamReader, StringReader }

sealed abstract class Csv {
  val format: CsvFormat
  val rows: Vector[Either[CsvError, CsvRow]]

  lazy val data: Vector[CsvRow] =
    rows.collect { case Right(row) => row }
  lazy val errors: Vector[CsvError] =
    rows.collect { case Left(error) => error }
  def hasErrors: Boolean = !errors.isEmpty

  def unlabeled: UnlabeledCsv = this match {
    case csv @ UnlabeledCsv(_, _) =>
      csv
    case LabeledCsv(format, _, rows) =>
      UnlabeledCsv(format.copy(header = false), rows)
  }

  def labeled: LabeledCsv = this match {
    case csv @ LabeledCsv(_, _, _) =>
      csv
    case UnlabeledCsv(format, rows) =>
      val format0 = format.copy(header = true)
      rows.headOption.flatMap(_.right.toOption).map { hdr =>
        LabeledCsv(format0, hdr.text(format), rows.tail)
      }.getOrElse {
        LabeledCsv(format0, Vector.empty, Vector.empty)
      }
  }

  override def toString: String = {
    val full = this match {
      case LabeledCsv(_, header, _) =>
        CsvRow(header map (CsvCell.Data(_))) +: data
      case UnlabeledCsv(_, _) =>
        data
    }

    full.iterator.
      map(_ render format).
      mkString(format.rowDelim.value)
  }
}

case class LabeledCsv(format: CsvFormat, header: Vector[String], rows: Vector[Either[CsvError, CsvRow]]) extends Csv

case class UnlabeledCsv(format: CsvFormat, rows: Vector[Either[CsvError, CsvRow]]) extends Csv

object Csv {
  val BufferSize = 32 * 1024

  def empty(format: CsvFormat): Csv =
    if (format.header) LabeledCsv(format, Vector.empty, Vector.empty)
    else UnlabeledCsv(format, Vector.empty)

  def parseReader(reader: Reader, format: CsvFormatStrategy = CsvFormat.Guess): Csv = {
    val (format0, reader0) = format match {
      case (guess: GuessCsvFormat) => guess(reader)
      case (fmt: CsvFormat) => (fmt, reader)
    }
    CsvParser(format0).parseReader(reader0)
  }

  def parseString(input: String, format: CsvFormatStrategy = CsvFormat.Guess): Csv =
    parseReader(new StringReader(input), format)

  def parseInputStream(is: InputStream, format: CsvFormatStrategy = CsvFormat.Guess, charset: Charset = StandardCharsets.UTF_8): Csv =
    parseReader(new InputStreamReader(is, charset), format)

  def parseFile(file: File, format: CsvFormatStrategy = CsvFormat.Guess, charset: Charset = StandardCharsets.UTF_8): Csv =
    parseInputStream(new FileInputStream(file), format, charset)

  def parsePath(filename: String, format: CsvFormatStrategy = CsvFormat.Guess, charset: Charset = StandardCharsets.UTF_8): Csv =
    parseFile(new File(filename), format, charset)
}
