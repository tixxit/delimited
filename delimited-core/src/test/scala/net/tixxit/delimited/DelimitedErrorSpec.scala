package net.tixxit.delimited

import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers
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
}
