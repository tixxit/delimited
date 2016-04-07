package net.tixxit.delimited

import java.io.{ BufferedWriter, ByteArrayInputStream, File, FileOutputStream, OutputStreamWriter }

import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop._

class DelimitedParserSpec extends WordSpec with Matchers with Checkers {
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
      csv shouldBe Vector(
        Right(Row("a", "b", "c,d")),
        Right(Row("e,f,g"))
      )
    }

    "parse escaped quotes" in {
      DelimitedParser(TestFormat).parseString(
        "a,'''','c'''|'''''d''''', ''''"
      ) shouldBe Vector(
        Right(Row("a", "'", "c'")),
        Right(Row("''d''", " ''''"))
      )
    }

    "respect DelimitedFormat separator" in {
      DelimitedParser(TestFormat).parseString("a,b,c|d,e,f") shouldBe
        DelimitedParser(TestFormat.withSeparator(";")).parseString("a;b;c|d;e;f")
    }

    "respect DelimitedFormat quote" in {
      DelimitedParser(TestFormat).parseString("'a,b','b'|d,e") shouldBe
        DelimitedParser(TestFormat.withQuote("^")).parseString("^a,b^,^b^|d,e")
    }

    "respect DelimitedFormat quote escape" in {
      DelimitedParser(TestFormat).parseString("'a''b',''''|' '") shouldBe
        DelimitedParser(TestFormat.withQuoteEscape("\\")).parseString("'a\\'b','\\''|' '")
    }

    "respect DelimitedFormat row delimiter" in {
      DelimitedParser(TestFormat).parseString("a,b|c,d|e,f") shouldBe
        DelimitedParser(TestFormat.withRowDelim(RowDelim.Unix)).parseString("a,b\nc,d\ne,f")
      DelimitedParser(TestFormat).parseString("a,b|c,d|e,f") shouldBe
        DelimitedParser(TestFormat.withRowDelim("%")).parseString("a,b%c,d%e,f")
    }

    "parse CSV with row delimiter in quote" in {
      DelimitedParser(TestFormat).parseString("a,'b|c'|'d|e',f") shouldBe Vector(
        Right(Row("a", "b|c")),
        Right(Row("d|e", "f")))
    }

    "parser respects whitespace" in {
      val data = " a , , 'a','b'|  b  ,c  ,   "
      val csv = DelimitedParser(DelimitedFormat.Guess.withRowDelim("|")).parseString(data)
      csv shouldBe Vector(
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

      rows1 shouldBe Vector.empty
      rows2 shouldBe Vector(Right(Row("a", "b", "c")))
      rows3 shouldBe Vector(Right(Row("d", "e", "f")))
    }

    "work over multiple inputs, where both yield 1 row" in {
      val parser0 = DelimitedParser(TestFormat)
      val (parser1, rows1) = parser0.parseChunk(Some("a,b,c|d"))
      val (parser2, rows2) = parser1.parseChunk(Some(",e,f"))
      val (parser3, rows3) = parser2.parseChunk(None)

      rows1 shouldBe Vector(Right(Row("a", "b", "c")))
      rows2 shouldBe Vector.empty
      rows3 shouldBe Vector(Right(Row("d", "e", "f")))
    }

    "work over multiple inputs, with an error in a row" in {
      val parser0 = DelimitedParser(TestFormat.withRowDelimInQuotes(false))

      // a,b,c|d,'e|',f|h,"i",j
      // 01234567890123456789012345
      //           1         2
      //      ^    ^   ^

      val (parser1, rows1) = parser0.parseChunk(Some("a,b,c|d"))
      val (parser2, rows2) = parser1.parseChunk(Some(",'e|',"))
      val (parser3, rows3) = parser2.parseChunk(Some("f|h,'i',j"))
      val (parser4, rows4) = parser3.parseChunk(None)

      rows1 shouldBe Vector(Right(Row("a", "b", "c")))
      val error1 = DelimitedError("Unmatched quoted string at row delimiter", 6, 10, "d,'e", 2, 5)
      rows2 shouldBe Vector(Left(error1))
      val error2 = DelimitedError("Unmatched quoted string at row delimiter", 11, 14, "',f", 3, 4)
      rows3 shouldBe Vector(Left(error2))
      rows4 shouldBe Vector(Right(Row("h", "i", "j")))
    }

    "error when unfinished quote at EOF" in {
      val parser = DelimitedParser(TestFormat)
      val rows = parser
        .parseChunk(Some("a,b,c|d,'e,f"))._1
        .parseChunk(None)._2
      val error = DelimitedError("Unmatched quoted string at end of file", 6, 12, "d,'e,f", 2, 7)
      rows shouldBe Vector(Left(error))
    }
  }

  val simpleCsv = List.fill(DelimitedParser.BufferSize)("a,'b',c").mkString("\n")

  "parseInputStream" should {
    "parse large, simple CSV" in {
      val parser = DelimitedParser(DelimitedFormat.Guess)
      val in = new ByteArrayInputStream(simpleCsv.getBytes("utf-8"))
      val rows = parser.parseInputStream(in).toVector
      rows.foreach { row =>
        row shouldBe Right(Row("a", "b", "c"))
      }
    }
  }

  "parseFile" should {
    "parse large, simple CSV" in {
      val file = File.createTempFile("simple", "csv") // create the fixture
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"))
      try {
        writer.write(simpleCsv)
      } finally {
        writer.close()
      }
      val parser = DelimitedParser(DelimitedFormat.Guess)
      val rows = parser.parseFile(file)
      rows.foreach { row =>
        row shouldBe Right(Row("a", "b", "c"))
      }
    }
  }
}
