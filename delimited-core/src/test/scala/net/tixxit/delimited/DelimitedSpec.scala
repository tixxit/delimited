package net.tixxit.delimited

import org.specs2.mutable._

class DelimitedSpec extends Specification {
  "DelimitedParser" should {
    "parse CSV with separator in quote" in {
      val data = """a,"b","c,d"|"e,f,g""""
      val csv = Delimited.parseString(data, DelimitedFormat.Guess.withRowDelim("|"))
      csv.unlabeled.rows must_== Vector(
        Right(Row.data("a", "b", "c,d")),
        Right(Row.data("e,f,g"))
      )
    }

    val TestFormat = DelimitedFormat(
      separator = ",",
      quote = "'",
      quoteEscape = "'",
      empty = "N/A",
      invalid = "N/M",
      header = false,
      rowDelim = RowDelim.Custom("|"),
      allowRowDelimInQuotes = true
    )

    "parse escaped quotes" in {
      Delimited.parseString(
        "a,'''','c'''|'''''d''''', ''''",
        TestFormat
      ).rows must_== Vector(
        Right(Row.data("a", "'", "c'")),
        Right(Row.data("''d''", " ''''"))
      )
    }

    "respect DelimitedFormat separator" in {
      Delimited.parseString("a,b,c|d,e,f", TestFormat).rows must_==
        Delimited.parseString("a;b;c|d;e;f", TestFormat.withSeparator(";")).rows
    }

    "respect DelimitedFormat quote" in {
      Delimited.parseString("'a,b','b'|d,e", TestFormat).rows must_==
        Delimited.parseString("^a,b^,^b^|d,e", TestFormat.withQuote("^")).rows
    }

    "respect DelimitedFormat quote escape" in {
      Delimited.parseString("'a''b',''''|' '", TestFormat).rows must_==
        Delimited.parseString("'a\\'b','\\''|' '", TestFormat.withQuoteEscape("\\")).rows
    }

    "respect DelimitedFormat empty" in {
      Delimited.parseString("a,N/A,b|N/A,N/A", TestFormat).rows must_==
        Delimited.parseString("a,,b|,", TestFormat.withEmpty("")).rows
    }

    "respect DelimitedFormat invalid" in {
      Delimited.parseString("a,N/M,b|N/M,N/M", TestFormat).rows must_==
        Delimited.parseString("a,nm,b|nm,nm", TestFormat.withInvalid("nm")).rows
    }

    "respect DelimitedFormat row delimiter" in {
      Delimited.parseString("a,b|c,d|e,f", TestFormat).rows must_==
        Delimited.parseString("a,b\nc,d\ne,f", TestFormat.withRowDelim(RowDelim.Unix)).rows
    }

    "parse CSV with row delimiter in quote" in {
      Delimited.parseString("a,'b|c'|'d|e',f", TestFormat).rows must_== Vector(
        Right(Row.data("a", "b|c")),
        Right(Row.data("d|e", "f")))
    }

    "parser respects whitespace" in {
      val data = " a , , 'a','b'|  b  ,c  ,   "
      val csv = Delimited.parseString(data, DelimitedFormat.Guess.withRowDelim("|"))
      csv.rows must_== Vector(
        Right(Row.data(" a ", " ", " 'a'", "b")),
        Right(Row.data("  b  ", "c  ", "   ")))
    }
  }
}
