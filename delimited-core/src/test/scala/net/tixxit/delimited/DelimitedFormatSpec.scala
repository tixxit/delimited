package net.tixxit.delimited

import java.io.{ Reader, StringReader }

import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop._

class DelimitedFormatSpec extends WordSpec with Matchers with Checkers {
  import Generators._

  "escape" should {
    //"wrap value in quotes" in {
    //  check { (format: DelimitedFormat, text: String) =>
    //    val escaped = format.escape(text)
    //    escaped.startsWith(format.quote) && escaped.endsWith(format.quote)
    //  }
    //}

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
    "escape quoted values" in {
      check { (format: DelimitedFormat, text: String) =>
        val rendered = format.render(text)
        val start = format.quote.length
        val end = rendered.length - 2 * start
        rendered.startsWith(format.quote) &&
          format.unescape(rendered.substring(start, end)) == text ||
          rendered == text
      }
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
        val format = DelimitedFormat.Guess.withRowDelim(rowDelim)
        format(text).rowDelim == rowDelim
      }
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
