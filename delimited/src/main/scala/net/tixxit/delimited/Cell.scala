package net.tixxit.delimited

sealed abstract class Cell {
  def render(format: DelimitedFormat): String
}

object Cell {
  case class Data(value: String) extends Cell {
    def render(format: DelimitedFormat): String = format.render(value)
    override def toString: String = value
  }
  case object Empty extends Cell {
    def render(format: DelimitedFormat): String = format.empty
    override def toString: String = "-"
  }
  case object Invalid extends Cell {
    def render(format: DelimitedFormat): String = format.invalid
    override def toString: String = "<error>"
  }
}
