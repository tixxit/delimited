package net.tixxit.delimited

/**
 * A single row in a CSV file.
 */
final class Row(val cells: Vector[String]) extends AnyVal {
  def text(format: DelimitedFormat): Vector[String] =
    cells.map(format.render)

  def render(format: DelimitedFormat): String =
    cells.iterator.map(format.render).mkString(format.separator)

  override def toString: String =
    cells.mkString("Row(", ", ", ")")
}

object Row extends (Vector[String] => Row) {
  def apply(cells: Vector[String]): Row = new Row(cells)
  def apply(cells: String*): Row = new Row(cells.toVector)
}
