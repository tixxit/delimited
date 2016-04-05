package net.tixxit.delimited
package iteratee

import cats.Applicative

import io.iteratee._
import io.iteratee.internal.Step

object Delimited {

  /**
   * An [[Enumeratee]] that parses chunks of character data from a delimited
   * file into [[Row]]s.
   *
   * @param format the strategy to use while parsing the delimited file
   */
  final def parseChunks[F[_]](format: DelimitedFormatStrategy)(implicit F: Applicative[F]): Enumeratee[F, String, Row] =
    new Enumeratee[F, String, Row] {
      def apply[A](step: Step[F, Row, A]): F[Step[F, String, Step[F, Row, A]]] =
        F.pure(doneOrLoop(DelimitedParser(format))(step))

      protected final def doneOrLoop[A](parser: DelimitedParser)(step: Step[F, Row, A]): Step[F, String, Step[F, Row, A]] = {
        if (step.isDone) {
          Step.done[F, String, Step[F, Row, A]](step)
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
