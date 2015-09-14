package net.tixxit.delimited

import java.nio.charset.{ Charset, StandardCharsets }
import java.io.File
import java.io.{ InputStream, FileInputStream }
import java.io.{ Reader, InputStreamReader, StringReader }

import net.tixxit.delimited.parser.DelimitedParser

sealed abstract class Delimited {
  import Delimited.{ Labeled, Unlabeled }

  val format: DelimitedFormat
  val rows: Vector[Either[DelimitedError, Row]]

  lazy val data: Vector[Row] =
    rows.collect { case Right(row) => row }
  lazy val errors: Vector[DelimitedError] =
    rows.collect { case Left(error) => error }
  def hasErrors: Boolean = !errors.isEmpty

  def unlabeled: Unlabeled = this match {
    case csv @ Unlabeled(_, _) =>
      csv
    case Labeled(format, _, rows) =>
      Unlabeled(format.copy(header = false), rows)
  }

  def labeled: Labeled = this match {
    case csv @ Labeled(_, _, _) =>
      csv
    case Unlabeled(format, rows) =>
      val format0 = format.copy(header = true)
      rows.headOption.flatMap(_.right.toOption).map { hdr =>
        Labeled(format0, hdr.text(format), rows.tail)
      }.getOrElse {
        Labeled(format0, Vector.empty, Vector.empty)
      }
  }

  override def toString: String = {
    val full = this match {
      case Labeled(_, header, _) =>
        Row(header: _*) +: data
      case Unlabeled(_, _) =>
        data
    }

    full.iterator.
      map(_ render format).
      mkString(format.rowDelim.value)
  }
}

object Delimited {

  case class Labeled(format: DelimitedFormat, header: Vector[String], rows: Vector[Either[DelimitedError, Row]]) extends Delimited
  
  case class Unlabeled(format: DelimitedFormat, rows: Vector[Either[DelimitedError, Row]]) extends Delimited

  def empty(format: DelimitedFormat): Delimited =
    if (format.header) Labeled(format, Vector.empty, Vector.empty)
    else Unlabeled(format, Vector.empty)

  private def wrap(format: DelimitedFormat, rows: Vector[Either[DelimitedError, Row]]): Delimited = {
    val csv = Unlabeled(format, rows)
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
