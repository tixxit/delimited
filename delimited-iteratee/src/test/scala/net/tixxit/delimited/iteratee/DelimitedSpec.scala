package net.tixxit.delimited
package iteratee

import java.io.{ BufferedWriter, ByteArrayInputStream, File, FileOutputStream, OutputStreamWriter }

import scala.util.{ Random, Success, Try }

import cats.{ Id, Eval }
import cats.data.EitherT
import cats.instances.try_._

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
      import io.iteratee.modules.id._
      val parts = enumList(Nil)
        .through(Delimited.formatString(DelimitedFormat.CSV))
        .into(Iteratee.consume)
      parts shouldBe Vector()
    }

    "render delimited file" in {
      import io.iteratee.modules.id._
      val rows = Stream(
        Row("a", "b", "c"),
        Row("d", ",", "a,b,c"),
        Row("\"blah\"", "x", "1234"),
        Row("", "q", "r"),
        Row("1", "2", "3")
      )
      val mkString: Iteratee[Id, String, String] =
        Iteratee.consume[Id, String].map(_.mkString)

      val file1 = Enumerator.enumStream[Id, Row](rows, chunkSize = 2)
        .through(Delimited.formatString(DelimitedFormat.CSV))
        .into(mkString)
      file1 shouldBe "a,b,c\nd,\",\",\"a,b,c\"\n\"\"\"blah\"\"\",x,1234\n,q,r\n1,2,3"
      val file2 = Enumerator.enumStream[Id, Row](rows, chunkSize = 2)
        .through(Delimited.formatString(DelimitedFormat.TSV))
        .into(mkString)
      file2 shouldBe "a\tb\tc\nd\t,\ta,b,c\n\"\"\"blah\"\"\"\tx\t1234\n\tq\tr\n1\t2\t3"
      val format = DelimitedFormat("-", quote = "%", rowDelim = RowDelim("|"))
      val file3 = Enumerator.enumStream[Id, Row](rows, chunkSize = 2)
        .into(mkString.through(Delimited.formatString(format)))
      file3 shouldBe "a-b-c|d-,-a,b,c|\"blah\"-x-1234|-q-r|1-2-3"
    }
  }

  "inferDelimitedFormat" should {
    "infer default format when no input given" in {
      import io.iteratee.modules.id._
      val format = enumList(Nil).into(Delimited.inferDelimitedFormat())
      val expected = DelimitedFormat.Guess("")
      format shouldBe expected
    }

    "output guess when zipped with parser" in {
      import io.iteratee.modules.try_._
      val parser: Iteratee[Try, String, Vector[Row]] =
        Delimited.parseString[Try, Throwable](DelimitedFormat.Guess).into(Iteratee.consume)

      List(20, 100).foreach { bufferSize =>
        val iteratee: Iteratee[Try, String, (DelimitedFormat, Vector[Row])] =
          Delimited.inferDelimitedFormat(bufferSize = bufferSize).zip(parser)
        val chunks = Enumerator.enumStream[Try, String](
          Stream("a,b,", "c\nd,e", ",", "f\n", "g,h,i\nj,k", ",", "l"),
          chunkSize = 2
        )
        val Success((format, rows)) = chunks.into(iteratee)
        format.separator shouldBe ","
        format.rowDelim.value shouldBe "\n"
        rows shouldBe Vector(Row("a", "b", "c"), Row("d", "e", "f"), Row("g", "h", "i"), Row("j", "k", "l"))
      }
    }
  }

  "parseString" should {
    "parse no chunks" in {
      import io.iteratee.modules.try_._
      val rows = enumList[String](Nil)
        .through(Delimited.parseString[Try, Throwable](DelimitedFormat.Guess))
        .into(Iteratee.consume)
      rows shouldBe Success(Vector.empty[Row])
    }

    "parse empty chunk" in {
      import io.iteratee.modules.try_._
      val rows = enumList[String]("" :: Nil)
        .through(Delimited.parseString[Try, Throwable](DelimitedFormat.CSV))
        .into(Iteratee.consume)
      rows shouldBe Success(Vector.empty[Row])
    }

    "parse only 2 rows" in {
      import io.iteratee.modules.try_._
      val rows = enumList[String](List("a,b,", "c\nd,e", ",", "f\n", "g,h,i\nj,k", "l"))
        .through(Delimited.parseString[Try, Throwable](DelimitedFormat.CSV))
        .into(Iteratee.take(2))
      rows shouldBe Success(Vector(Row("a", "b", "c"), Row("d", "e", "f")))
    }

    "parse header, then rest" in {
      import io.iteratee.modules.try_._

      val withHeader: Iteratee[Try, Row, (Row, Vector[Row])] = for {
        header <- Iteratee.head
        rest   <- Iteratee.consume
      } yield (header.get -> rest)
      val rows = enumList[String](List("a,b,", "c\nd,e", ",", "f\n", "g,h,i"))
        .through(Delimited.parseString[Try, Throwable](DelimitedFormat.CSV))
        .into(withHeader)
      rows shouldBe Success((Row("a", "b", "c"), Vector(Row("d", "e", "f"), Row("g", "h", "i"))))
    }

    "parse larges files, in chunks" in {
      import io.iteratee.modules.eitherT._

      type EvalOr[A] = EitherT[Eval, Throwable, A]

      // This is basically my paranoid test case. So, we have a large
      // delimited file (guaranteed to pass inference buffer size) that
      // is split into a bunch of small chunks. We then sequence our
      // enumeratee through an Iteratee for good measure to ensure our
      // EOF behaviour works as expected.
      val grouped = Delimited.parseString[EvalOr, Throwable](DelimitedFormat.Guess).andThen(Enumeratee.grouped(2))
      val consumeGroups = grouped.into(Iteratee.consume)
      val expected = (1 to 100000)
        .map(_.toString)
        .grouped(3)
        .map { cells => Row(cells: _*) }
        .toVector
      val file = expected.map(_.render(DelimitedFormat.CSV)).mkString("\n")
      val actual = Enumerator
        .enumStream[EvalOr, String](split(file).toStream, chunkSize = 1)
        .into(consumeGroups).value.value.right.map(_.flatten)
      actual shouldBe Right(expected)
    }
  }
}
