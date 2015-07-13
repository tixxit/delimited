package net.tixxit.delimited

import java.nio.charset.{ Charset, StandardCharsets }
import java.io.File
import java.io.{ InputStream, FileInputStream }
import java.io.{ Reader, InputStreamReader, StringReader }

import net.tixxit.delimited.parser.DelimitedParser

sealed abstract class Delimited {
  val format: DelimitedFormat
  val rows: Vector[Either[DelimitedError, Row]]

  lazy val data: Vector[Row] =
    rows.collect { case Right(row) => row }
  lazy val errors: Vector[DelimitedError] =
    rows.collect { case Left(error) => error }
  def hasErrors: Boolean = !errors.isEmpty

  def unlabeled: UnlabeledDelimited = this match {
    case csv @ UnlabeledDelimited(_, _) =>
      csv
    case LabeledDelimited(format, _, rows) =>
      UnlabeledDelimited(format.copy(header = false), rows)
  }

  def labeled: LabeledDelimited = this match {
    case csv @ LabeledDelimited(_, _, _) =>
      csv
    case UnlabeledDelimited(format, rows) =>
      val format0 = format.copy(header = true)
      rows.headOption.flatMap(_.right.toOption).map { hdr =>
        LabeledDelimited(format0, hdr.text(format), rows.tail)
      }.getOrElse {
        LabeledDelimited(format0, Vector.empty, Vector.empty)
      }
  }

  override def toString: String = {
    val full = this match {
      case LabeledDelimited(_, header, _) =>
        Row(header map (Cell.Data(_))) +: data
      case UnlabeledDelimited(_, _) =>
        data
    }

    full.iterator.
      map(_ render format).
      mkString(format.rowDelim.value)
  }
}

case class LabeledDelimited(format: DelimitedFormat, header: Vector[String], rows: Vector[Either[DelimitedError, Row]]) extends Delimited

case class UnlabeledDelimited(format: DelimitedFormat, rows: Vector[Either[DelimitedError, Row]]) extends Delimited

object Delimited {
  def empty(format: DelimitedFormat): Delimited =
    if (format.header) LabeledDelimited(format, Vector.empty, Vector.empty)
    else UnlabeledDelimited(format, Vector.empty)

  private def wrap(format: DelimitedFormat, rows: Vector[Either[DelimitedError, Row]]): Delimited = {
    val csv = UnlabeledDelimited(format, rows)
    if (format.header) csv.labeled else csv
  }

  def parseReader(reader: Reader, format: DelimitedFormatStrategy = DelimitedFormat.Guess): Delimited = {
    val (format0, reader0) = format match {
      case (guess: GuessDelimitedFormat) => guess(reader)
      case (fmt: DelimitedFormat) => (fmt, reader)
    }
    wrap(format0, DelimitedParser(format0).parseReader(reader0))
  }

  def parseString(input: String, format: DelimitedFormatStrategy = DelimitedFormat.Guess): Delimited =
    parseReader(new StringReader(input), format)

  def parseInputStream(is: InputStream, format: DelimitedFormatStrategy = DelimitedFormat.Guess, charset: Charset = StandardCharsets.UTF_8): Delimited =
    parseReader(new InputStreamReader(is, charset), format)

  def parseFile(file: File, format: DelimitedFormatStrategy = DelimitedFormat.Guess, charset: Charset = StandardCharsets.UTF_8): Delimited = {
    val is = new FileInputStream(file)
    try {
      parseInputStream(is, format, charset)
    } finally {
      is.close()
    }
  }

  def parsePath(filename: String, format: DelimitedFormatStrategy = DelimitedFormat.Guess, charset: Charset = StandardCharsets.UTF_8): Delimited =
    parseFile(new File(filename), format, charset)
}
