package net.tixxit.csv

import org.specs2.mutable._

class CsvSpec extends Specification {
  "CsvParser" should {
    "parse CSV with separator in quote" in {
      val data = """a,"b","c,d"|"e,f,g""""
      val csv = Csv.parseString(data, CsvFormat.Guess.withRowDelim("|"))
      csv.unlabeled.rows must_== Vector(
        Right(CsvRow.data("a", "b", "c,d")),
        Right(CsvRow.data("e,f,g"))
      )
    }

    val TestFormat = CsvFormat(
      separator = ",",
      quote = "'",
      quoteEscape = "'",
      empty = "N/A",
      invalid = "N/M",
      header = false,
      rowDelim = CsvRowDelim.Custom("|"),
      allowRowDelimInQuotes = true
    )

    "parse escaped quotes" in {
      Csv.parseString(
        "a,'''','c'''|'''''d''''', ''''",
        TestFormat
      ).rows must_== Vector(
        Right(CsvRow.data("a", "'", "c'")),
        Right(CsvRow.data("''d''", " ''''"))
      )
    }

    "respect CsvFormat separator" in {
      Csv.parseString("a,b,c|d,e,f", TestFormat).rows must_==
        Csv.parseString("a;b;c|d;e;f", TestFormat.withSeparator(";")).rows
    }

    "respect CsvFormat quote" in {
      Csv.parseString("'a,b','b'|d,e", TestFormat).rows must_==
        Csv.parseString("^a,b^,^b^|d,e", TestFormat.withQuote("^")).rows
    }

    "respect CsvFormat quote escape" in {
      Csv.parseString("'a''b',''''|' '", TestFormat).rows must_==
        Csv.parseString("'a\\'b','\\''|' '", TestFormat.withQuoteEscape("\\")).rows
    }

    "respect CsvFormat empty" in {
      Csv.parseString("a,N/A,b|N/A,N/A", TestFormat).rows must_==
        Csv.parseString("a,,b|,", TestFormat.withEmpty("")).rows
    }

    "respect CsvFormat invalid" in {
      Csv.parseString("a,N/M,b|N/M,N/M", TestFormat).rows must_==
        Csv.parseString("a,nm,b|nm,nm", TestFormat.withInvalid("nm")).rows
    }

    "respect CsvFormat row delimiter" in {
      Csv.parseString("a,b|c,d|e,f", TestFormat).rows must_==
        Csv.parseString("a,b\nc,d\ne,f", TestFormat.withRowDelim(CsvRowDelim.Unix)).rows
    }

    "parse CSV with row delimiter in quote" in {
      Csv.parseString("a,'b|c'|'d|e',f", TestFormat).rows must_== Vector(
        Right(CsvRow.data("a", "b|c")),
        Right(CsvRow.data("d|e", "f")))
    }

    "parser respects whitespace" in {
      val data = " a , , 'a','b'|  b  ,c  ,   "
      val csv = Csv.parseString(data, CsvFormat.Guess.withRowDelim("|"))
      csv.rows must_== Vector(
        Right(CsvRow.data(" a ", " ", " 'a'", "b")),
        Right(CsvRow.data("  b  ", "c  ", "   ")))
    }
  }
}
