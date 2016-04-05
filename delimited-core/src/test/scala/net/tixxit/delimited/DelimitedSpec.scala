package net.tixxit.delimited

import org.specs2.mutable._

class DelimitedParserSpec extends Specification {
  val TestFormat = DelimitedFormat(
    separator = ",",
    quote = "'",
    quoteEscape = "'",
    rowDelim = RowDelim("|"),
    allowRowDelimInQuotes = true
  )

  "parseString" should {
    "parse CSV with separator in quote" in {
      val data = """a,"b","c,d"|"e,f,g""""
      val csv = DelimitedParser(DelimitedFormat.Guess.withRowDelim("|")).parseString(data)
      csv must_== Vector(
        Right(Row("a", "b", "c,d")),
        Right(Row("e,f,g"))
      )
    }

    "parse escaped quotes" in {
      DelimitedParser(TestFormat).parseString(
        "a,'''','c'''|'''''d''''', ''''"
      ) must_== Vector(
        Right(Row("a", "'", "c'")),
        Right(Row("''d''", " ''''"))
      )
    }

    "respect DelimitedFormat separator" in {
      DelimitedParser(TestFormat).parseString("a,b,c|d,e,f") must_==
        DelimitedParser(TestFormat.withSeparator(";")).parseString("a;b;c|d;e;f")
    }

    "respect DelimitedFormat quote" in {
      DelimitedParser(TestFormat).parseString("'a,b','b'|d,e") must_==
        DelimitedParser(TestFormat.withQuote("^")).parseString("^a,b^,^b^|d,e")
    }

    "respect DelimitedFormat quote escape" in {
      DelimitedParser(TestFormat).parseString("'a''b',''''|' '") must_==
        DelimitedParser(TestFormat.withQuoteEscape("\\")).parseString("'a\\'b','\\''|' '")
    }

    "respect DelimitedFormat row delimiter" in {
      DelimitedParser(TestFormat).parseString("a,b|c,d|e,f") must_==
        DelimitedParser(TestFormat.withRowDelim(RowDelim.Unix)).parseString("a,b\nc,d\ne,f")
    }

    "parse CSV with row delimiter in quote" in {
      DelimitedParser(TestFormat).parseString("a,'b|c'|'d|e',f") must_== Vector(
        Right(Row("a", "b|c")),
        Right(Row("d|e", "f")))
    }

    "parser respects whitespace" in {
      val data = " a , , 'a','b'|  b  ,c  ,   "
      val csv = DelimitedParser(DelimitedFormat.Guess.withRowDelim("|")).parseString(data)
      csv must_== Vector(
        Right(Row(" a ", " ", " 'a'", "b")),
        Right(Row("  b  ", "c  ", "   ")))
    }
  }

  "parseChunk" should {
    "work over multiple inputs, where one yields 0 rows" in {
      val parser0 = DelimitedParser(TestFormat)
      val (parser1, rows1) = parser0.parseChunk(Some("a,b,"))
      val (parser2, rows2) = parser1.parseChunk(Some("c|d,e,f"))
      val (parser3, rows3) = parser2.parseChunk(None)

      rows1 must_== Vector.empty
      rows2 must_== Vector(Right(Row("a", "b", "c")))
      rows3 must_== Vector(Right(Row("d", "e", "f")))
    }

    "work over multiple inputs, where both yield 1 row" in {
      val parser0 = DelimitedParser(TestFormat)
      val (parser1, rows1) = parser0.parseChunk(Some("a,b,c|d"))
      val (parser2, rows2) = parser1.parseChunk(Some(",e,f"))
      val (parser3, rows3) = parser2.parseChunk(None)

      rows1 must_== Vector(Right(Row("a", "b", "c")))
      rows2 must_== Vector.empty
      rows3 must_== Vector(Right(Row("d", "e", "f")))
    }
  }
}
