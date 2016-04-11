package net.tixxit.delimited
package iteratee

import java.nio.charset.{ Charset, StandardCharsets }

import cats.{ Applicative, Monad }

import io.iteratee._
import io.iteratee.internal.{ Step, Input }

/**
 * A collection of [[Iteratee]]s and [[Enumeratee]]s for working with delimited
 * data (TSV, CSV, etc).
 */
object Delimited {

  /**
   * Formats rows as a delimited file of the provided format. The output can
   * be written to a file as-is.
   */
  final def formatString[F[_]: Monad](format: DelimitedFormat): Enumeratee[F, Row, String] =
    Enumeratee
      .map[F, Row, String](_.render(format))
      .andThen(Enumeratee.intersperse(format.rowDelim.value))

  /**
   * An [[Iteratee]] that will attempt to consume at least `bufferSize`
   * characters of the input to infer the `DelimitedFormat` of the input.  If
   * less than `bufferSize` characters are available in the input, then this
   * will use whatever has been seen so far, even if that is 0.
   *
   * @param bufferSize minimum amount of chars to use when inferring format
   */
  final def inferDelimitedFormat[F[_]](
    bufferSize: Int = DelimitedParser.BufferSize
  )(implicit F: Applicative[F]): Iteratee[F, String, DelimitedFormat] = {
    def stepWith(acc: Vector[String], length: Int): Step[F, String, DelimitedFormat] =
      if (length >= bufferSize) {
        val chunk = acc.mkString
        val format = DelimitedFormat.Guess(chunk)
        Step.done(format)
      } else {
        new Step.PureCont[F, String, DelimitedFormat] {
          final def onEl(e: String): Step[F, String, DelimitedFormat] =
            stepWith(acc :+ e, length + e.length)

          final def onChunk(h1: String, h2: String, t: Vector[String]): Step[F, String, DelimitedFormat] = {
            val tLength = t.foldLeft(0) { (n, e) => n + e.length }
            val newLength = length + h1.length + h2.length + tLength
            stepWith((acc :+ h1 :+ h2) ++ t, newLength)
          }

          final def run: F[DelimitedFormat] = {
            F.pure(DelimitedFormat.Guess(acc.mkString))
          }
        }
      }

    Iteratee.fromStep(stepWith(Vector.empty, 0))
  }

  /**
   * An [[Enumeratee]] that parses chunks of character data from a delimited
   * file into [[Row]]s.
   *
   * @param format the strategy to use while parsing the delimited file
   */
  final def parseString[F[_]](
    format: DelimitedFormatStrategy
  )(implicit F: Applicative[F]): Enumeratee[F, String, Row] = {
    new Enumeratee[F, String, Row] {
      def apply[A](step: Step[F, Row, A]): F[Step[F, String, Step[F, Row, A]]] =
        F.pure(doneOrLoop(DelimitedParser(format))(step))

      private[this] def doneOrLoop[A](parser: DelimitedParser)(step: Step[F, Row, A]): Step[F, String, Step[F, Row, A]] = {
        if (step.isDone) {
          val (leftOver, _) = parser.reset
          Step.doneWithLeftoverInput[F, String, Step[F, Row, A]](step, Input.el(leftOver))
        } else {
          stepWith(parser, step)
        }
      }

      private[this] def stepWith[A](
        parser: DelimitedParser,
        step: Step[F, Row, A]
      ): Step[F, String, Step[F, Row, A]] = {
        new Step.Cont[F, String, Step[F, Row, A]] {
          final def run: F[Step[F, Row, A]] = parseChunk(None)._2

          final def onEl(chunk: String): F[Step[F, String, Step[F, Row, A]]] = {
            val (nextParser, nextStep) = parseChunk(Some(chunk))
            F.map(nextStep)(doneOrLoop(nextParser))
          }

          final def onChunk(h1: String, h2: String, t: Vector[String]): F[Step[F, String, Step[F, Row, A]]] = {
            val bldr = new StringBuilder
            bldr.append(h1)
            bldr.append(h2)
            t.foreach(bldr.append)
            val chunk = bldr.toString
            onEl(chunk)
          }

          private[this] def parseChunk(chunk: Option[String]): (DelimitedParser, F[Step[F, Row, A]]) = {
            val (nextParser, results) = parser.parseChunk(chunk)
            val rows = results.map(_.right.get)
            val nextStep = rows match {
              case Vector() => F.pure(step)
              case Vector(e) => step.feedEl(e)
              case _ => step.feedChunk(rows(0), rows(1), rows.drop(2))
            }
            (nextParser, nextStep)
          }
        }
      }
    }
  }
}
