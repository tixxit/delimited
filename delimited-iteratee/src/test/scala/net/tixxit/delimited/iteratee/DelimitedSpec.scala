package net.tixxit.delimited
package iteratee

import java.io.{ BufferedWriter, ByteArrayInputStream, File, FileOutputStream, OutputStreamWriter }

import scala.util.Random

import cats.{ Id, Eval }

import io.iteratee.{ Enumeratee, Enumerator, Iteratee }

import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop._

class DelimitedSpec extends WordSpec with Matchers with Checkers {
  def split(chunk: String, rng: Random, height: Int): List[String] = {
    if (chunk.length < 4 || height <= 0) {
      chunk :: Nil
    } else {
      val i = rng.nextInt(chunk.length)
      val init = chunk.substring(0, i)
      val tail = chunk.substring(i)
      split(init, rng, height - 1) ++ split(tail, rng, height - 1)
    }
  }

  def split(chunk: String): List[String] = {
    val height = (math.log(chunk.length) / math.log(2)).toInt - 2
    split(chunk, Random, height)
  }

  "parseChunks" should {
    "parse no chunks" in {
      import io.iteratee.pure._
      val rows = enumList[String](Nil)
        .mapE(Delimited.parseChunks[Id](DelimitedFormat.Guess))
        .run(Iteratee.consume)
      rows shouldBe Vector.empty[Row]
    }

    "parse empty chunk" in {
      import io.iteratee.pure._
      val rows = enumList[String]("" :: Nil)
        .mapE(Delimited.parseChunks[Id](DelimitedFormat.CSV))
        .run(Iteratee.consume)
      rows shouldBe Vector.empty[Row]
    }

    "parse only 2 rows" in {
      import io.iteratee.pure._
      val rows = enumList[String](List("a,b,", "c\nd,e", ",", "f\n", "g,h,i\nj,k", "l"))
        .mapE(Delimited.parseChunks[Id](DelimitedFormat.CSV))
        .run(Iteratee.take(2))
      rows shouldBe Vector(Row("a", "b", "c"), Row("d", "e", "f"))
    }

    "parse larges files, in chunks" in {
      import io.iteratee.eval._

      // This is basically my paranoid test case. So, we have a large
      // delimited file (guaranteed to pass inference buffer size) that
      // is split into a bunch of small chunks. We then sequence our
      // enumeratee through an Iteratee for good measure to ensure our
      // EOF behaviour works as expected.
      val grouped = Delimited.parseChunks[Eval](DelimitedFormat.Guess).andThen(Enumeratee.grouped(2))
      val consumeGroups = Iteratee.consume[Eval, Vector[Row]].through(grouped)
      val expected = (1 to 100000)
        .map(_.toString)
        .grouped(3)
        .map { cells => Row(cells: _*) }
        .toVector
      val file = expected.map(_.render(DelimitedFormat.CSV)).mkString("\n")
      val actual = enumList(split(file)).run(consumeGroups).value.flatten
      actual shouldBe expected
    }
  }
}
