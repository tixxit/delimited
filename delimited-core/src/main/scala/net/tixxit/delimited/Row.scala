package net.tixxit.delimited

import scala.collection.{ AbstractSeq, IndexedSeq, IndexedSeqLike }
import scala.collection.mutable.{ ArrayBuilder, Builder }

/**
 * A single row in a CSV file.
 */
final class Row(private val cells: Array[String]) extends Seq[String]
with IndexedSeq[String] with IndexedSeqLike[String, Row] {
  def apply(idx: Int): String = cells(idx)

  def length: Int = cells.length

  override def seq: Row = this

  override def newBuilder: Builder[String, Row] = Row.newBuilder

  def text(format: DelimitedFormat): Vector[String] =
    cells.map(format.render)(collection.breakOut)

  def render(format: DelimitedFormat): String =
    cells.iterator.map(format.render).mkString(format.separator)

  override def toString: String =
    cells.mkString("Row(", ", ", ")")
}

object Row {
  def apply(cells: String*): Row = new Row(cells.toArray)

  def unapplySeq(row: Row): Option[Seq[String]] = Some(row.cells)

  def newBuilder: Builder[String, Row] =
    ArrayBuilder.make[String]().mapResult(new Row(_))
}
