package net.tixxit.delimited

import scala.collection.{ AbstractSeq, IndexedSeq }
import scala.collection.mutable.{ ArrayBuilder, Builder }

/**
 * A row in a delimited file, as a sequence of *unescaped* strings. The values
 * must be rendered first to be used in a delimited file. This provides fast,
 * random access to the underlying cells in the row and convenience methods for
 * rendering the row given a [[DelimitedFormat]].
 */
final class Row private[delimited] (private val cells: Array[String]) extends Iterable[String] {
  def apply(idx: Int): String = cells(idx)

  def length: Int = cells.length

  override def size: Int = cells.length

  def iterator: Iterator[String] = cells.iterator

  /**
   * Returns a Vector of the *rendered* cells of this row.
   *
   * @param format the format to use when rendering the cells
   */
  def text(format: DelimitedFormat): Vector[String] =
    cells.iterator.map(format.render).toVector

  /**
   * Returns this row rendered to a String using the given format. This will
   * not add any row delimiters; it renders the cells then joins them together
   * using `format.separator`.
   */
  def render(format: DelimitedFormat): String =
    cells.iterator.map(format.render).mkString(format.separator)

  override def toString: String =
    cells.mkString("Row(", ", ", ")")
}

object Row {

  /**
   * Returns a new `Row` from a sequence unescaped cells.
   */
  def apply(cells: String*): Row = new Row(cells.toArray)

  /**
   * This will construct a new `Row` that wraps an array of strings. This will
   * not make a defensive copy - any mutations to the provided array will be
   * reflected in `Row`. Since `Row` is expected to be immutable, mutating the
   * array after calling this method is considered unsafe and cause
   * unpredictable results. You have been warned.
   */
  def fromArray(cells: Array[String]): Row = new Row(cells)

  def unapplySeq(row: Row): Option[Seq[String]] = Some(row.cells)

  def newBuilder: Builder[String, Row] =
    ArrayBuilder.make[String].mapResult(new Row(_))
}
