package net.tixxit.delimited

import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop._

class PartialFormatSpec extends WordSpec with Matchers with Checkers {
  "merge" should {
    "merge sorted streams in sorted order" in {
      check { (lhs: List[Int], rhs: List[Int]) =>
        val s = PartialFormat.merge(lhs.sorted.toStream, rhs.sorted.toStream)
        s.toList.sliding(2).forall {
          case x :: y :: Nil => x <= y
          case _ => true
        }
      }
    }

    "handle infinite streams" in {
      val lhs = Stream.from(1).filter(_ % 2 == 1)
      val rhs = Stream.from(1).filter(_ % 2 == 0)
      PartialFormat.merge(lhs, rhs).take(100).toList shouldBe List.range(1, 101)
    }
  }

  "crossWith" should {
    "output sorted stream" in {
      check { (lhs0: List[Int], rhs0: List[Int]) =>
        val lhs = lhs0.sorted.toStream
        val rhs = rhs0.sorted.toStream
        val result = PartialFormat.crossWith(lhs, rhs) { (x, y) =>
          x.toLong + y.toLong
        }
        result.toList.sliding(2).forall {
          case x :: y :: Nil => x <= y
          case _ => true
        }
      }
    }

    "be permutation of cross product" in {
      check { (lhs0: List[Int], rhs0: List[Int]) =>
        val lhs = lhs0.sorted.toStream
        val rhs = rhs0.sorted.toStream
        val actual = PartialFormat.crossWith(lhs, rhs)(_ -> _).toSet
        val expected = lhs0.flatMap { x => rhs.map((x, _)) }.toSet
        actual == expected
      }
    }
  }
}
