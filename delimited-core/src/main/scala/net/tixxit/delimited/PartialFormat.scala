package net.tixxit.delimited

import PartialFormat._

case class PartialFormat(
  separator: Option[String] = None,
  quote: Option[String] = None,
  quoteEscape: Option[String] = None,
  rowDelim: Option[RowDelim] = None,
  allowRowDelimInQuotes: Option[Boolean] = None,
  separatorCandidates: Candidates[String] = DefaultSeparatorCandidates,
  quoteCandidates: Candidates[String] = DefaultQuoteCandidates,
  quoteEscapeCandidates: Candidates[String => String] = DefaultQuoteEscapeCandidates,
  rowDelimCandidates: Candidates[RowDelim] = DefaultRowDelimCandidates,
  allowRowDelimInQuoteCandidates: Candidates[Boolean] = DefaultAllowRowDelimInQuotesCandidates,
  parseBudget: Option[Int] = None
) extends GuessDelimitedFormat {

  def withSeparator(separator: String): PartialFormat = copy(separator = Some(separator))
  def withQuote(quote: String): PartialFormat = copy(quote = Some(quote))
  def withQuoteEscape(quoteEscape: String): PartialFormat = copy(quoteEscape = Some(quoteEscape))
  def withRowDelim(rowDelim: RowDelim): PartialFormat = copy(rowDelim = Some(rowDelim))
  def withRowDelim(rowDelim: String): PartialFormat = copy(rowDelim = Some(RowDelim(rowDelim)))
  def withRowDelimInQuotes(allowRowDelimInQuotes: Boolean): PartialFormat = copy(allowRowDelimInQuotes = Some(allowRowDelimInQuotes))

  /**
   * Performs a very naive guess of the DelimitedFormat. This uses weighted
   * frequencies of occurences of common separators, row-delimiters, quotes,
   * quote escapes, etc. and simply selects the max for each.
   *
   * This supports:
   *
   *   * \r\n and \n as row delimiters,
   *   * ',', '\t', ';', and '|' as field delimiters,
   *   * '"', and ''' as quote delimiter,
   *   * the quote delimiter or \ for quote escapes.
   */
  def apply(str: String): DelimitedFormat = {
    val scores = candidates(str)
      .map { case Weighted(w2, format) =>
        val rows: Vector[Either[DelimitedError, Row]] =
          maybeInit(DelimitedParser(format).parseString(str))
        val w1 = rank(rows)
        format -> (w1, w2)
      }

    val bestScores = parseBudget.map(scores.take(_)).getOrElse(scores)
    bestScores.find(_._2._1 == 1.0) match {
      case Some((format, _)) => format
      case None => bestScores.maxBy(_._2)._1
    }
  }

  private def maybeInit[A](rows: Vector[A]): Vector[A] =
    if (rows.size > 2) rows.init
    else rows

  /**
   * Rank parse results based on some super subjective criteria. We prefer
   * results whose rows generally have a uniform number of columns, but aren't
   * too thin, either column-wise or row-wise.
   */
  def rank(results: Vector[Either[DelimitedError, Row]]): Double = {
    val rows = results.collect { case Right(row) => row }
    if (rows.size == 0 || rows.size != results.size) {
      0.0
    } else {
      val lengths = rows.map(_.length)
      val histo = lengths.foldLeft(Map.empty[Int, Double]) { (acc, n) =>
        acc + (n -> (acc.getOrElse(n, 0D) + 1D))
      }
      val (mode, count) = histo.maxBy(_._2)

      // Is there more than 1 column?
      val colScore = if (mode > 1) 1 else 0
      // Is there more than 1 row?
      val rowScore = if (rows.length > 1) 1 else 0
      // Are the rows generally of uniform length?
      val uniformScore = if ((rows.length - count) <= math.log(rows.length) / 2) 1 else 0

      (colScore + rowScore + uniformScore) / 3.0
    }
  }

  /**
   * Returns a stream of weighted candidate [[DelimitedFormat]]s, in order of the
   * belief that they'll parse `str` successfully.
   */
  def candidates(str: String): Stream[Weighted[DelimitedFormat]] = {
    def count(ndl: String): Int = {
      def check(i: Int, j: Int = 0): Boolean =
        if (j >= ndl.length) true
        else if (i < str.length && str.charAt(i) == ndl.charAt(j)) check(i + 1, j + 1)
        else false

      def loop(i: Int, cnt: Int): Int =
        if (i < str.length) {
          loop(i + 1, if (check(i)) cnt + 1 else cnt)
        } else cnt

      loop(0, 0)
    }

    def getOrElse[A: Ordering](a: Option[A])(candidates: => Candidates[A]): Stream[Weighted[A]] =
      a.fold(candidates.toStream.sorted) { a => Stream(Weighted(1.0, a)) }

    val weightedRowDelims = getOrElse(rowDelim) {
      rowDelimCandidates.map { case Weighted(w, rowDelim) =>
        val cnt = count(rowDelim.value) + rowDelim.alternate.fold(0)(count) + 1
        Weighted(w * cnt, rowDelim)
      }
    }
    val weightedSeparators = getOrElse(separator) {
      separatorCandidates.map { case Weighted(w, sep) =>
        Weighted(w * (count(sep) + 1), sep)
      }
    }
    val weightedQuotes: Stream[Weighted[(String, String)]] = (quote, quoteEscape) match {
      case (Some(q), Some(escaped)) =>
        Stream(Weighted(1.0, (q, escaped)))
      case (Some(q), None) =>
        quoteEscapeCandidates.map { case Weighted(w, escape) =>
          val escaped = escape(q)
          Weighted(w * (count(escaped + q) + 1), (q, escaped))
        }.sorted.toStream
      case (None, Some(escaped)) =>
        quoteCandidates.map { case weightedQuote @ Weighted(w, q) =>
          Weighted(w * (count(q) + 1), (q, escaped))
        }.sorted.toStream
      case (None, None) =>
        val combined = for {
          Weighted(w0, q) <- quoteCandidates
          Weighted(w1, escape) <- quoteEscapeCandidates
        } yield {
          val escaped = escape(q)
          val w = w0 * w1 * (count(q) + 1) * (count(escaped + q) + 1)
          Weighted(w, (q, escaped))
        }
        combined.sorted.toStream
    }
    val weightedAllowRowDelimInQuotes: Stream[Weighted[Boolean]] =
      getOrElse(allowRowDelimInQuotes)(allowRowDelimInQuoteCandidates)

    val t1 = crossWith(weightedQuotes, weightedRowDelims)(_ zip _)
    val t2 = crossWith(t1, weightedSeparators)(_ zip _)
    val t3 = crossWith(t2, weightedAllowRowDelimInQuotes)(_ zip _)
    t3.map { case Weighted(w, ((((quote, quoteEscape), rowDelim), separator), allowRowDelimInQuotes)) =>
      Weighted(w, DelimitedFormat(separator, quote, quoteEscape, rowDelim, allowRowDelimInQuotes))
    }
  }
}

object PartialFormat {
  type Candidates[A] = List[Weighted[A]]

  val DefaultSeparatorCandidates: Candidates[String] = List(
    Weighted(2.0, ","),
    Weighted(3.0, "\t"),
    Weighted(2.0, ";"),
    Weighted(1.0, "|")
  )

  val DefaultQuoteCandidates: Candidates[String] = List(
    Weighted(1.2, "\""),
    Weighted(1.0, "\'")
  )

  val DefaultQuoteEscapeCandidates: Candidates[String => String] = List(
    Weighted(1.1, quote => quote),
    Weighted(1.0, quote => "\\")
  )

  val DefaultRowDelimCandidates: Candidates[RowDelim] = List(
    Weighted(1.0, RowDelim.Unix),
    Weighted(2.0, RowDelim.Windows),
    Weighted(0.95, RowDelim.Both)
  )

  val DefaultAllowRowDelimInQuotesCandidates: Candidates[Boolean] = List(
    Weighted(1.0, false),
    Weighted(1.0, true)
  )

  /**
   * Merges 2 streams, sorted by `f`, in sorted order. The resulting stream
   * contains all the same elements as both `lhs` and `rhs`.
   */
  def merge[A](lhs: Stream[A], rhs: Stream[A])(implicit ord: Ordering[A]): Stream[A] =
    (lhs, rhs) match {
      case (a0 #:: as, a1 #:: _) if ord.lteq(a0, a1) =>
        a0 #:: merge(as, rhs)
      case (a0 #:: _, a1 #:: as) =>
        a1 #:: merge(lhs, as)
      case (Stream.Empty, _) => rhs
      case _ => lhs
    }

  /**
   * Given that `f(lhs(i), rhs(j)) >= f(lhs(i + 1), rhs(j))` and
   * `f(lhs(i), rhs(j)) >= f(lhs(i), rhs(j + 1))` for all i, j, then this will
   * cross `lhs` with `rhs`, gauranteeing that the returned stream is in order.
   */
  def crossWith[A, B, C](lhs: Stream[A], rhs: Stream[B])(f: (A, B) => C)(implicit ord: Ordering[C]): Stream[C] = {
    (lhs, rhs) match {
      case (a #:: as, b #:: bs) =>
        f(a, b) #:: merge(bs.map(f(a, _)), crossWith(as, rhs)(f))
      case _ =>
        Stream.empty
    }
  }

  case class Weighted[A](weight: Double, value: A) {
    def zip[B](that: Weighted[B]): Weighted[(A, B)] =
      Weighted(weight * that.weight, (value, that.value))
  }

  object Weighted {
    implicit def weightedOrdering[A](implicit ord: Ordering[A]): Ordering[Weighted[A]] =
      new Ordering[Weighted[A]] {
        def compare(x: Weighted[A], y: Weighted[A]): Int = {
          val cmp = java.lang.Double.compare(y.weight, x.weight)
          if (cmp == 0) {
            ord.compare(x.value, y.value)
          } else {
            cmp
          }
        }
      }
  }
}
