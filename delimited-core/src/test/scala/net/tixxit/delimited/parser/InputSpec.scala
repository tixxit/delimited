package net.tixxit.delimited.parser

import java.io.{ Reader, StringReader }

import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop._

class InputSpec extends WordSpec with Matchers with Checkers {
  val in1 = Input.init("abc")
  val in2 = in1.append("def").marked(2).append("xyz")

  "init" should {
    "not mark Input as last" in (Input.init("xyz").isLast shouldBe false)
    "set mark to 0" in (Input.init("xyz1").mark shouldBe 0)
    "view data" in (Input.init("abc").window shouldBe "abc")
  }

  "append" should {
    "trim data up-to mark" in {
      val in = Input.init("abc").marked(1).append("def")
      in.data shouldBe "bcdef"
      in.window shouldBe in.data
    }

    "trim data for marks in the future" in {
      in1.marked(10).append("def").window shouldBe ""
      in1.marked(10).append("def").append("ghi").append("jkl").append("").data shouldBe "kl"
    }
  }

  "charAt" should {
    "throw an IndexOutOfBoundsException on negative indices" in {
      an [IndexOutOfBoundsException] should be thrownBy in1.charAt(-1)
      an [IndexOutOfBoundsException] should be thrownBy in2.charAt(-1)
      an [IndexOutOfBoundsException] should be thrownBy in1.charAt(-99)
    }

    "throw an IndexOutOfBoundsException on unseen data" in {
      an [IndexOutOfBoundsException] should be thrownBy in1.charAt(3)
      an [IndexOutOfBoundsException] should be thrownBy in2.charAt(9)
      an [IndexOutOfBoundsException] should be thrownBy in1.charAt(99)
      an [IndexOutOfBoundsException] should be thrownBy in1.charAt(Long.MaxValue)
    }

    "return character in window" in {
      in2.charAt(2) shouldBe 'c'
      in2.charAt(3) shouldBe 'd'
      in2.charAt(8) shouldBe 'z'
      in1.charAt(0) shouldBe 'a'
      in1.charAt(2) shouldBe 'c'
    }

    "throw an IndexOutOfBoundsException on removed indices" in {
      an [IndexOutOfBoundsException] should be thrownBy in2.charAt(0)
      an [IndexOutOfBoundsException] should be thrownBy in2.charAt(1)
    }

    "return character in window, but before mark" in {
      in2.marked(4).charAt(2) shouldBe 'c'
      in1.marked(2).charAt(0) shouldBe 'a'
    }
  }

  "limit" should {
    "return index of first unreadable character" in {
      Input.init("").limit shouldBe 0
      in1.limit shouldBe 3
      in2.limit shouldBe 9
    }
  }

  "substring" should {
    "return substring from stream indices" in {
      in2.substring(4, 7) shouldBe "efx"
      in2.substring(2, 3) shouldBe "c"
      in2.substring(2, 2) shouldBe ""
      in1.substring(1, 2) shouldBe "b"
    }
  }

  "marked" should {
    "set mark in new Input" in {
      in1.marked(3).mark shouldBe 3
      in2.marked(6).mark shouldBe 6
    }

    "allow large marks" in {
      in1.marked(10).mark shouldBe 10
      in1.append("def").marked(10).mark shouldBe 10
    }

    "throw IllegalArgumentException on backward marks" in {
      an [IllegalArgumentException] should be thrownBy in2.marked(1)
      an [IllegalArgumentException] should be thrownBy in1.marked(1).marked(0)
    }
  }

  "finished" should {
    "mark input as last" in {
      in1.finished.isLast shouldBe true
      in2.finished.isLast shouldBe true
    }
  }
}
