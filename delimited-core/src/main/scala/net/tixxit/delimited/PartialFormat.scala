package net.tixxit.delimited

case class PartialFormat(
    separator: Option[String] = None,
    quote: Option[String] = None,
    quoteEscape: Option[String] = None,
    rowDelim: Option[RowDelim] = None,
    allowRowDelimInQuotes: Boolean = true
  ) extends GuessDelimitedFormat {

  def withSeparator(separator: String): PartialFormat = copy(separator = Some(separator))
  def withQuote(quote: String): PartialFormat = copy(quote = Some(quote))
  def withQuoteEscape(quoteEscape: String): PartialFormat = copy(quoteEscape = Some(quoteEscape))
  def withRowDelim(rowDelim: RowDelim): PartialFormat = copy(rowDelim = Some(rowDelim))
  def withRowDelim(rowDelim: String): PartialFormat = copy(rowDelim = Some(RowDelim(rowDelim)))
  def withRowDelimInQuotes(allowRowDelimInQuotes: Boolean): PartialFormat = copy(allowRowDelimInQuotes = allowRowDelimInQuotes)

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

    def choose(weightedOptions: (String, Double)*)(f: String => Int): String = {
      val weights = Map(weightedOptions: _*)
      val weightedCounts = weights.map { case (c, w) => (c, w * f(c)) }
      val (best, weight) = weightedCounts.maxBy(_._2)
      val candidates = weightedCounts.collect {
        case (c, w) if weight == w => c
      }
      // Amongst all equally good choices, choose the one with the best a
      // prior weight.
      candidates.maxBy(weights)
    }

    val rowDelim0 = rowDelim.getOrElse {
      val windCnt = count("\r\n")
      val unixCnt = count("\n") - windCnt

      if ((windCnt < 4 * unixCnt) && (unixCnt < 4 * windCnt)) RowDelim.Both
      else if (windCnt < 4 * unixCnt) RowDelim.Unix
      else RowDelim.Windows
    }
    val separator0 = separator.getOrElse {
      choose(","  -> 2.0, "\t" -> 3.0, ";"  -> 2.0, "|"  -> 1.0)(count)
    }
    val quote0 = quote.getOrElse(choose("\"" -> 1.2, "\'" -> 1)(count))
    val quoteEscape0 = quoteEscape.getOrElse(choose(s"$quote0$quote0" -> 1.1, s"\\$quote0" -> 1)(count).dropRight(quote0.length))

    DelimitedFormat(separator0, quote0, quoteEscape0, rowDelim0, allowRowDelimInQuotes)
  }
}

object PartialFormat {

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
