package net.tixxit.delimited

import org.scalatest.{ WordSpec, Matchers }
import org.scalatestplus.scalacheck.Checkers
import org.scalacheck._

class RowSpec extends WordSpec with Matchers with Checkers {
  "fromArray" should {
    "construct Row from array" in {
      check { (xs: Array[String]) =>
        Row.fromArray(xs).toVector == xs.toVector
      }
    }
  }

  "unapplySeq" should {
    "match the elements in a Row" in {
      check { (xs: Vector[String]) =>
        Row(xs: _*) match {
          case Row(ys @ _*) => ys == xs
          case _ => false
        }
      }
    }
  }

  "text" should {
    "return rendered cells" in {
      val row = Row(",", "abc", "\t", "\"", "\n")
      row.text(DelimitedFormat.CSV) shouldBe Vector("\",\"", "abc", "\t", "\"\"\"\"", "\"\n\"")
      row.text(DelimitedFormat.TSV) shouldBe Vector(",", "abc", "\"\t\"", "\"\"\"\"", "\"\n\"")
    }
  }

  "render" should {
    "render the entire row" in {
      val row = Row(",", "abc", "\t", "\"", "\n")
      row.render(DelimitedFormat.CSV) shouldBe "\",\",abc,\t,\"\"\"\",\"\n\""
      row.render(DelimitedFormat.TSV) shouldBe ",\tabc\t\"\t\"\t\"\"\"\"\t\"\n\""
    }
  }

  "toString" should {
    "be entirely unsurprising" in {
      // Entirely shameless juicing of code coverage...
      val row = Row("a", "b", "c")
      assert(row.toString == "Row(a, b, c)")
      val row2 = Row(row.toList.take(2): _*)
      assert(row2.toString == "Row(a, b)")
    }
  }
}
