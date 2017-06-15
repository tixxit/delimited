package net.tixxit.delimited
package iteratee

import java.nio.charset.{ Charset, StandardCharsets }

import cats.{ ApplicativeError, Monad }
import cats.data.NonEmptyList
import cats.instances.either._
import cats.instances.vector._
import cats.syntax.flatMap._
import cats.syntax.traverse._

import io.iteratee._
import io.iteratee.internal.Step

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
  )(implicit F: Monad[F]): Iteratee[F, String, DelimitedFormat] =
    Enumeratee.scan[F, String, (List[String], Int)]((Nil, 0)) {
      case ((acc, length), input) => (input :: acc, length + input.length)
    }.andThen(Enumeratee.takeWhile(_._2 <= bufferSize)).into(Iteratee.last).map {
      case Some((acc, _)) => DelimitedFormat.Guess(acc.reverse.mkString)
      case None           => DelimitedFormat.Guess("")
    }

  /**
   * An [[Enumeratee]] that parses chunks of character data from a delimited
   * file into [[Row]]s.
   *
   * @param format         strategy used to determine the format
   * @param bufferSize     minimum size of buffer used for format inference
   * @param maxCharsPerRow hard limit on the # of chars in a row, or 0 if there
   *                       is no limit
   */
  final def parseString[F[_], E >: DelimitedError](
    format: DelimitedFormatStrategy,
    bufferSize: Int = DelimitedParser.BufferSize,
    maxCharsPerRow: Int = 0
  )(implicit F: ApplicativeError[F, E]): Enumeratee[F, String, Row] = {
    new Enumeratee[F, String, Row] {
      def apply[A](step: Step[F, Row, A]): F[Step[F, String, Step[F, Row, A]]] =
        F.pure(doneOrLoop(DelimitedParser(format, bufferSize, maxCharsPerRow))(step))

      private[this] def doneOrLoop[A](parser: DelimitedParser)(step: Step[F, Row, A]): Step[F, String, Step[F, Row, A]] = {
        if (step.isDone) {
          val (leftOver, _) = parser.reset
          Step.doneWithLeftovers[F, String, Step[F, Row, A]](step, leftOver :: Nil)
        } else {
          stepWith(parser, step)
        }
      }

      private[this] def stepWith[A](parser: DelimitedParser, step: Step[F, Row, A]): Step[F, String, Step[F, Row, A]] =
        new Step.Cont[F, String, Step[F, Row, A]] {
          final def run: F[Step[F, Row, A]] = F.map(parseChunk(None))(_._2)

          final def feedEl(chunk: String): F[Step[F, String, Step[F, Row, A]]] = {
            F.map(parseChunk(Some(chunk))) {
              case (nextParser, nextStep) => doneOrLoop(nextParser)(nextStep)
            }
          }

          final protected def feedNonEmpty(chunk: Seq[String]): F[Step[F, String, Step[F, Row, A]]] = {
            val bldr = new StringBuilder
            chunk.foreach(bldr.append)
            feedEl(bldr.toString)
          }

          private[this] def parseChunk(chunk: Option[String]): F[(DelimitedParser, Step[F, Row, A])] = {
            val (nextParser, results) = parser.parseChunk(chunk)

            results.sequenceU match {
              case Right(rows) => F.map(step.feed(rows))((nextParser, _))
              case Left(error) => F.raiseError(error)
            }
          }
        }
    }
  }
}
