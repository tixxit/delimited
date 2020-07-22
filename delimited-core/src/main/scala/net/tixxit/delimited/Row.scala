package net.tixxit.delimited

import java.util.Arrays

import scala.collection.{ AbstractSeq, IndexedSeq }
import scala.collection.mutable.{ ArrayBuilder, Builder }
import scala.util.hashing.MurmurHash3

/**
 * A row in a delimited file, as a sequence of *unescaped* strings. The values
 * must be rendered first to be used in a delimited file. This provides fast,
 * random access to the underlying cells in the row and convenience methods for
 * rendering the row given a [[DelimitedFormat]].
 */
final class Row private[delimited] (private val cells: Array[String]) extends (Int => String) {
  def apply(idx: Int): String = cells(idx)

  def length: Int = cells.length

  def foreach(f: String => Unit): Unit = {
    var idx = 0
    while (idx < cells.length) {
      f(cells(idx))
      idx += 1
    }
  }

  def size: Int = length

  def iterator: Iterator[String] = cells.iterator

  def toVector: Vector[String] = {
    val bldr = Vector.newBuilder[String]
    bldr.sizeHint(length)
    bldr ++= cells
    bldr.result()
  }

  def toList: List[String] = iterator.toList

  def toIndexedSeq: IndexedSeq[String] = cells.toIndexedSeq

  /**
   * Returns a Vector of the *rendered* cells of this row.
   *
   * @param format the format to use when rendering the cells
   */
  def text(format: DelimitedFormat): Vector[String] = {
    val bldr = Vector.newBuilder[String]
    bldr.sizeHint(length)
    var idx = 0
    while (idx < cells.length) {
      bldr += format.render(cells(idx))
      idx += 1
    }
    bldr.result()
  }

  /**
   * Returns this row rendered to a String using the given format. This will
   * not add any row delimiters; it renders the cells then joins them together
   * using `format.separator`.
   */
  def render(format: DelimitedFormat): String = {
    val strbuilder = new java.lang.StringBuilder()
    var idx = 0
    while (idx < cells.length) {
      if (idx > 0) strbuilder.append(format.separator)
      strbuilder.append(format.render(cells(idx)))
      idx += 1
    }
    strbuilder.toString()
  }

  override def toString: String =
    cells.mkString("Row(", ", ", ")")

  override def hashCode: Int = MurmurHash3.arrayHash(cells, 5347)

  override def equals(that: Any): Boolean = that match {
    case (that: Row) =>
      Arrays.equals(cells.asInstanceOf[Array[AnyRef]], that.cells.asInstanceOf[Array[AnyRef]])
    case _ => false
  }
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

  def unapplySeq(row: Row): Option[Seq[String]] = Some(row.cells.toIndexedSeq)

  def newBuilder: Builder[String, Row] =
    ArrayBuilder.make[String].mapResult(new Row(_))
}
