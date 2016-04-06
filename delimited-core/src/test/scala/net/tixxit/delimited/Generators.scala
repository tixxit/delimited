package net.tixxit.delimited

import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary

object Generators {
  implicit def arbDelimitedFormat: Arbitrary[DelimitedFormat] =
    Arbitrary(genDelimitedFormat)

  implicit def arbRowDelim: Arbitrary[RowDelim] =
    Arbitrary(genRowDelim)

  def genSeparator: Gen[String] =
    Gen.frequency(
      5 -> Gen.const(","),
      5 -> Gen.const("\t"),
      5 -> Gen.const("|"),
      5 -> Gen.const(";"),
      5 -> Gen.const("\u0000"),
      5 -> arbitrary[Char].map(_.toString),
      3 -> Gen.listOfN(2, arbitrary[Char]).map(_.mkString),
      1 -> Gen.listOfN(3, arbitrary[Char]).map(_.mkString)
    )

  def genQuote: Gen[String] =
    Gen.frequency(
      5 -> Gen.const("\""),
      5 -> Gen.const("'"),
      5 -> Gen.const("`"),
      5 -> Gen.const("''"),
      5 -> arbitrary[Char].map(_.toString),
      3 -> Gen.listOfN(2, arbitrary[Char]).map(_.mkString),
      1 -> Gen.listOfN(3, arbitrary[Char]).map(_.mkString)
    )

  def genQuoteEscape: Gen[String] =
    Gen.frequency(5 -> genQuote, 1 -> "\\")

  def genRowDelim0: Gen[String] =
    Gen.frequency(
      5 -> Gen.const("\n"),
      5 -> Gen.const("\r\n"),
      5 -> Gen.const("|"),
      5 -> Gen.const("\u0000"),
      5 -> arbitrary[Char].map(_.toString),
      3 -> Gen.listOfN(2, arbitrary[Char]).map(_.mkString),
      1 -> Gen.listOfN(3, arbitrary[Char]).map(_.mkString)
    )

  def genRowDelim: Gen[RowDelim] =
    Gen.oneOf(
      Gen.const(RowDelim.Unix),
      Gen.const(RowDelim.Windows),
      Gen.const(RowDelim.Both),
      for {
        primary   <- genRowDelim0
        secondary <- Gen.option(genRowDelim0)
      } yield RowDelim(primary, secondary)
    )

  def genDelimitedFormat: Gen[DelimitedFormat] =
    for {
      separator <- genSeparator
      quote <- genQuote
      if quote != separator
      quoteEscape <- genQuoteEscape
      if quoteEscape != separator
      rowDelim <- genRowDelim
      if rowDelim.value != separator &&
         rowDelim.alternate != Some(separator) &&
         rowDelim.value != quote &&
         rowDelim.alternate != Some(quote) &&
         rowDelim.value != quoteEscape &&
         rowDelim.alternate != Some(quoteEscape)
      allowRowDelimInQuotes <- arbitrary[Boolean]
    } yield {
      DelimitedFormat(
        separator = separator,
        quote = quote,
        quoteEscape = quoteEscape,
        rowDelim = rowDelim,
        allowRowDelimInQuotes = allowRowDelimInQuotes
      )
    }
}
