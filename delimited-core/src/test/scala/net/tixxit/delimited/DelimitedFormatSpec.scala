package net.tixxit.delimited

import java.io.{ Reader, StringReader }

import org.scalatest.{ WordSpec, Matchers }
import org.scalatestplus.scalacheck.Checkers
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop._

class DelimitedFormatSpec extends WordSpec with Matchers with Checkers {
  import Generators._

  "escape" should {
    "escape quotes" in {
      DelimitedFormat(",", quote = "'", quoteEscape = "'")
        .escape("'a'b''c'") shouldBe "''a''b''''c''"
    }

    "round-trip through unescape" in {
      check { (format: DelimitedFormat, text: String) =>
        text == format.unescape(format.escape(text))
      }
    }
  }

  "unescape" should {
    "unescape quotes" in {
      DelimitedFormat(",", quote = "'", quoteEscape = "'")
        .unescape("''a''b''''c''") shouldBe "'a'b''c'"
    }
  }

  "render" should {
    "round-trip through unquote" in {
      check { (format: DelimitedFormat, text: String) =>
        val rendered = format.render(text)
        val start = format.quote.length
        val end = rendered.length - 2 * start
        format.unquote(rendered) == text
      }
    }

    "quote values with separator in them" in {
      check { (format: DelimitedFormat) =>
        val quoted = format.render(format.separator)
        (
          quoted == s"${format.quote}${format.separator}${format.quote}" &&
          format.unquote(quoted) == format.separator
        )
      }
    }

    "quote values with quote in them" in {
      check { (format: DelimitedFormat) =>
        val quoted = format.render(format.quote)
        (
          quoted == s"${format.quote}${format.quoteEscape}${format.quote}${format.quote}" &&
          format.unquote(quoted) == format.quote
        )
      }
    }

    "quote values with primary row delimiter in them" in {
      check { (format: DelimitedFormat) =>
        val quoted = format.render(format.rowDelim.value)
        (
          quoted == s"${format.quote}${format.rowDelim.value}${format.quote}" &&
          format.unquote(quoted) == format.rowDelim.value
        )
      }
    }

    "quote values with secondary row delimiter in them" in {
      check(Prop.forAll(genDelimitedFormat.filter(_.rowDelim.alternate.nonEmpty)) { format =>
        val rowDelim2 = format.rowDelim.alternate.get
        val quoted = format.render(rowDelim2)
        (
          quoted == s"${format.quote}${rowDelim2}${format.quote}" &&
          format.unquote(quoted) == rowDelim2
        )
      })
    }
  }

  "Partial" should {
    "fix separator with withSeparator" in {
      check { (fmt: DelimitedFormat, text: String) =>
        val format = DelimitedFormat.Guess.withSeparator(fmt.separator)
        format(text).separator == fmt.separator
      }
    }

    "fix quote with withQuote" in {
      check { (fmt: DelimitedFormat, text: String) =>
        val format = DelimitedFormat.Guess.withQuote(fmt.quote)
        format(text).quote == fmt.quote
      }
    }

    "fix quoteEscape with withQuoteEscape" in {
      check { (fmt: DelimitedFormat, text: String) =>
        val format = DelimitedFormat.Guess.withQuoteEscape(fmt.quoteEscape)
        format(text).quoteEscape == fmt.quoteEscape
      }
    }

    "fix rowDelim with withRowDelim" in {
      check { (rowDelim: RowDelim, text: String) =>
        val format1 = DelimitedFormat.Guess.withRowDelim(rowDelim)
        val format2 = DelimitedFormat.Guess.withRowDelim(rowDelim.value)
        (
          format1(text).rowDelim == rowDelim &&
          format2(text).rowDelim == RowDelim(rowDelim.value)
        )
      }
    }

    "fix allowRowDelimInQuotes with withRowDelimInQuotes" in {
      check { (fmt: DelimitedFormat, text: String) =>
        val format = DelimitedFormat.Guess.withRowDelimInQuotes(fmt.allowRowDelimInQuotes)
        format(text).allowRowDelimInQuotes == fmt.allowRowDelimInQuotes
      }
    }

    "choose arbitrary best when choice not obvious" in {
      // We prioritize tabs over commas at 3:2 ratio. So, all else equal
      // (eg 3 * # tabs == 2 * # commas), we'd expect tabs to be chosen.
      val text1 = "a,b\t,c\nd\te,f"
      val format1 = DelimitedFormat.Guess(text1)
      format1.separator shouldBe "\t"

      // Commas are preferred 2:1 over pipes.
      val text2 = """a|b,c|d\ne|f,g|h"""
      val format2 = DelimitedFormat.Guess(text2)
      format2.separator shouldBe ","
    }

    "use RowDelim.Both when carriage return is sometimes used" in {
      val text1 = List.fill(10)("a,b\r\nc,d").mkString("\n")
      val text2 = List.fill(10)("a,b\r\nc,d\r\ne,f\r\ng,h").mkString("\n")
      val text3 = List.fill(10)("a,b\nc,d\ne,f").mkString("\r\n")
      DelimitedFormat.Guess(text1).rowDelim shouldBe RowDelim.Both
      DelimitedFormat.Guess(text2).rowDelim shouldBe RowDelim.Both
      DelimitedFormat.Guess(text3).rowDelim shouldBe RowDelim.Both

      val text4 = List.fill(10)("a,b").mkString("\n")
      DelimitedFormat.Guess(text4).rowDelim shouldBe RowDelim.Unix
      val text5 = List.fill(10)("a,b").mkString("\r\n")
      DelimitedFormat.Guess(text5).rowDelim shouldBe RowDelim.Windows
    }
  }

  "GuessDelimitedFormat" should {
    "return equivalent reader with format" in {
      check { (text: String) =>
        val (fmt, reader) = DelimitedFormat.Guess(new StringReader(text))
        readAll(reader) == text
      }
    }

    "work with large readers" in {
      val text = List.fill(DelimitedParser.BufferSize)("a,b,c").mkString("\n")
      val (fmt, reader) = DelimitedFormat.Guess(new StringReader(text))
      fmt.separator == "," &&
      fmt.rowDelim.value == "\n" &&
      readAll(reader) == text
    }
  }

  "toString" should {
    "print human-readable versions of RowDelim" in {
      DelimitedFormat.CSV.withRowDelim(RowDelim.Unix).toString shouldBe
        "DelimitedFormat(separator = \",\", quote = \"\"\", quoteEscape = \"\"\", rowDelim = Unix, allowRowDelimInQuotes = true)"
      DelimitedFormat.CSV.withRowDelim(RowDelim.Windows).toString shouldBe
        "DelimitedFormat(separator = \",\", quote = \"\"\", quoteEscape = \"\"\", rowDelim = Windows, allowRowDelimInQuotes = true)"
      DelimitedFormat.CSV.withRowDelim(RowDelim.Both).toString shouldBe
        "DelimitedFormat(separator = \",\", quote = \"\"\", quoteEscape = \"\"\", rowDelim = Unix+Windows, allowRowDelimInQuotes = true)"
      DelimitedFormat.CSV.withRowDelim("|").toString shouldBe
        "DelimitedFormat(separator = \",\", quote = \"\"\", quoteEscape = \"\"\", rowDelim = RowDelim(\"|\"), allowRowDelimInQuotes = true)"
      DelimitedFormat.CSV.withRowDelim(RowDelim("|", Some("-"))).toString shouldBe
        "DelimitedFormat(separator = \",\", quote = \"\"\", quoteEscape = \"\"\", rowDelim = RowDelim(\"|\", Some(\"-\")), allowRowDelimInQuotes = true)"
    }
  }

  private def readAll(reader: Reader): String = {
    val bldr = new StringBuilder
    val buf = new Array[Char](1024)
    var n = reader.read(buf)
    while (n >= 0) {
      bldr ++= new String(buf, 0, n)
      n = reader.read(buf)
    }
    bldr.toString
  }
}
