package net.tixxit.delimited

import org.scalatest.{ WordSpec, Matchers }
import org.scalatestplus.scalacheck.Checkers
import org.scalacheck._

class DelimitedErrorSpec extends WordSpec with Matchers with Checkers {

  "description" should {
    "print out nice, human readable description" in {
      val error = DelimitedError("the message", 1020, 1023, "a,b,c,d,e,f,g,h", 47, 3)
      error.description shouldBe
        """Error parsing CSV at row 47, column 3: the message
          |
          |a,b,c,d,e,f,g,h
          |  ^""".stripMargin
    }
  }

  "toString" should {
    "include location info" in {
      val error = DelimitedError("blah", 4, 32, "qwerty", 99, 88)
      error.toString shouldBe "DelimitedError(blah, rowStart = 4, pos = 32, context = qwerty, row = 99, col = 88)"
    }
  }
}
