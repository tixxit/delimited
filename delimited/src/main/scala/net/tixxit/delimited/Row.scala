package net.tixxit.delimited

/**
 * A single row in a CSV file.
 */
final class Row(val cells: Vector[Cell]) extends AnyVal {
  def text(format: DelimitedFormat): Vector[String] = cells.map(_ render format)

  def render(format: DelimitedFormat): String =
    cells.iterator map (_ render format) mkString format.separator

  override def toString: String =
    cells.mkString("Row(", ", ", ")")
}

object Row extends (Vector[Cell] => Row) {
  def apply(cells: Vector[Cell]): Row = new Row(cells)
  def apply(cells: Cell*): Row = new Row(Vector(cells: _*))

  def data(cells: String*): Row =
    new Row(cells.map(Cell.Data(_))(collection.breakOut))
}
