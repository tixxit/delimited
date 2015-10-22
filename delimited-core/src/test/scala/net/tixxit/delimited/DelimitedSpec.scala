package net.tixxit.delimited

import org.specs2.mutable._

class DelimitedSpec extends Specification {
  "DelimitedParser" should {
    "parse CSV with separator in quote" in {
      val data = """a,"b","c,d"|"e,f,g""""
      val csv = Delimited.parseString(data, DelimitedFormat.Guess.withRowDelim("|"))
      csv.unlabeled.rows must_== Vector(
        Right(Row("a", "b", "c,d")),
        Right(Row("e,f,g"))
      )
    }

    val TestFormat = DelimitedFormat(
      separator = ",",
      quote = "'",
      quoteEscape = "'",
      header = false,
      rowDelim = RowDelim.Custom("|"),
      allowRowDelimInQuotes = true
    )

    "parse escaped quotes" in {
      Delimited.parseString(
        "a,'''','c'''|'''''d''''', ''''",
        TestFormat
      ).rows must_== Vector(
        Right(Row("a", "'", "c'")),
        Right(Row("''d''", " ''''"))
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

    "respect DelimitedFormat row delimiter" in {
      Delimited.parseString("a,b|c,d|e,f", TestFormat).rows must_==
        Delimited.parseString("a,b\nc,d\ne,f", TestFormat.withRowDelim(RowDelim.Unix)).rows
    }

    "parse CSV with row delimiter in quote" in {
      Delimited.parseString("a,'b|c'|'d|e',f", TestFormat).rows must_== Vector(
        Right(Row("a", "b|c")),
        Right(Row("d|e", "f")))
    }

    "parser respects whitespace" in {
      val data = " a , , 'a','b'|  b  ,c  ,   "
      val csv = Delimited.parseString(data, DelimitedFormat.Guess.withRowDelim("|"))
      csv.rows must_== Vector(
        Right(Row(" a ", " ", " 'a'", "b")),
        Right(Row("  b  ", "c  ", "   ")))
    }
  }
}
