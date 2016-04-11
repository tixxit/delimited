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

  "formatString" should {
    "render empty file" in {
      import io.iteratee.pure._
      val parts = enumList(Nil)
        .mapE(Delimited.formatString(DelimitedFormat.CSV))
        .run(Iteratee.consume)
      parts shouldBe Vector()
    }

    "render delimited file" in {
      import io.iteratee.pure._
      val rows = Stream(
        Row("a", "b", "c"),
        Row("d", ",", "a,b,c"),
        Row("\"blah\"", "x", "1234"),
        Row("", "q", "r"),
        Row("1", "2", "3")
      )
      val mkString: Iteratee[Id, String, String] =
        Iteratee.consume.map(_.mkString)

      val file1 = Enumerator.enumStream(rows, chunkSize = 2)
        .mapE(Delimited.formatString(DelimitedFormat.CSV))
        .run(mkString)
      file1 shouldBe "a,b,c\nd,\",\",\"a,b,c\"\n\"\"\"blah\"\"\",x,1234\n,q,r\n1,2,3"
      val file2 = Enumerator.enumStream(rows, chunkSize = 2)
        .mapE(Delimited.formatString(DelimitedFormat.TSV))
        .run(mkString)
      file2 shouldBe "a\tb\tc\nd\t,\ta,b,c\n\"\"\"blah\"\"\"\tx\t1234\n\tq\tr\n1\t2\t3"
      val format = DelimitedFormat("-", quote = "%", rowDelim = RowDelim("|"))
      val file3 = Enumerator.enumStream(rows, chunkSize = 2)
        .run(mkString.through(Delimited.formatString(format)))
      file3 shouldBe "a-b-c|d-,-a,b,c|\"blah\"-x-1234|-q-r|1-2-3"
    }
  }

  "inferDelimitedFormat" should {
    "infer default format when no input given" in {
      import io.iteratee.pure._
      val format = enumList(Nil).run(Delimited.inferDelimitedFormat())
      val expected = DelimitedFormat.Guess("")
      format shouldBe expected
    }

    "output guess when zipped with parser" in {
      import io.iteratee.pure._
      val parser: Iteratee[Id, String, Vector[Row]] =
        Iteratee.consume.through(Delimited.parseString(DelimitedFormat.Guess))

      List(10, 100).foreach { bufferSize =>
        val iteratee: Iteratee[Id, String, (DelimitedFormat, Vector[Row])] =
          Delimited.inferDelimitedFormat(bufferSize = bufferSize).zip(parser)
        val chunks = Enumerator.enumStream[Id, String](
          Stream("a,b,", "c\nd,e", ",", "f\n", "g,h,i\nj,k", ",", "l"),
          chunkSize = 2
        )
        val (format, rows) = chunks.run(iteratee)
        format.separator shouldBe ","
        format.rowDelim.value shouldBe "\n"
        rows shouldBe Vector(Row("a", "b", "c"), Row("d", "e", "f"), Row("g", "h", "i"), Row("j", "k", "l"))
      }
    }
  }

  "parseString" should {
    "parse no chunks" in {
      import io.iteratee.pure._
      val rows = enumList[String](Nil)
        .mapE(Delimited.parseString[Id](DelimitedFormat.Guess))
        .run(Iteratee.consume)
      rows shouldBe Vector.empty[Row]
    }

    "parse empty chunk" in {
      import io.iteratee.pure._
      val rows = enumList[String]("" :: Nil)
        .mapE(Delimited.parseString[Id](DelimitedFormat.CSV))
        .run(Iteratee.consume)
      rows shouldBe Vector.empty[Row]
    }

    "parse only 2 rows" in {
      import io.iteratee.pure._
      val rows = enumList[String](List("a,b,", "c\nd,e", ",", "f\n", "g,h,i\nj,k", "l"))
        .mapE(Delimited.parseString[Id](DelimitedFormat.CSV))
        .run(Iteratee.take(2))
      rows shouldBe Vector(Row("a", "b", "c"), Row("d", "e", "f"))
    }

    "parse header, then rest" in {
      import io.iteratee.pure._

      val withHeader: Iteratee[Id, Row, (Row, Vector[Row])] = for {
        header <- Iteratee.head
        rest   <- Iteratee.consume
      } yield (header.get -> rest)
      val rows = enumList[String](List("a,b,", "c\nd,e", ",", "f\n", "g,h,i"))
        .mapE(Delimited.parseString[Id](DelimitedFormat.CSV))
        .run(withHeader)
      rows shouldBe (Row("a", "b", "c"), Vector(Row("d", "e", "f"), Row("g", "h", "i")))
    }

    "parse larges files, in chunks" in {
      import io.iteratee.eval._

      // This is basically my paranoid test case. So, we have a large
      // delimited file (guaranteed to pass inference buffer size) that
      // is split into a bunch of small chunks. We then sequence our
      // enumeratee through an Iteratee for good measure to ensure our
      // EOF behaviour works as expected.
      val grouped = Delimited.parseString[Eval](DelimitedFormat.Guess).andThen(Enumeratee.grouped(2))
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
