package net.tixxit.csv

sealed abstract class CsvCell {
  def render(format: CsvFormat): String
}

object CsvCell {
  case class Data(value: String) extends CsvCell {
    def render(format: CsvFormat): String = format.render(value)
    override def toString: String = value
  }
  case object Empty extends CsvCell {
    def render(format: CsvFormat): String = format.empty
    override def toString: String = "-"
  }
  case object Invalid extends CsvCell {
    def render(format: CsvFormat): String = format.invalid
    override def toString: String = "<error>"
  }
}
