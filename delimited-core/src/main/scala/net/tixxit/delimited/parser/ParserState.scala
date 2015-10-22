package net.tixxit.delimited
package parser

sealed trait ParserState {
  import ParserState._

  def rowStart: Long
  def readFrom: Long
  def input: Input
  def sizeHint: Int

  def newRowBuilder: scala.collection.mutable.Builder[String, Row] = {
    val bldr = Row.newBuilder
    if (sizeHint > 0)
      bldr.sizeHint(sizeHint)
    bldr
  }

  def withInput(input0: Input): ParserState = this match {
    case ContinueRow(_, _, partial, _, _) => ContinueRow(rowStart, readFrom, partial, input0, sizeHint)
    case SkipRow(_, _, _, _) => SkipRow(rowStart, readFrom, input0, sizeHint)
    case ParseRow(_, _, _, _) => ParseRow(rowStart, readFrom, input0, sizeHint)
  }

  def mapInput(f: Input => Input): ParserState = withInput(f(input))
}

object ParserState {
  case class ContinueRow(rowStart: Long, readFrom: Long, partial: Row, input: Input, sizeHint: Int = -1) extends ParserState
  case class SkipRow(rowStart: Long, readFrom: Long, input: Input, sizeHint: Int = -1) extends ParserState
  case class ParseRow(rowStart: Long, readFrom: Long, input: Input, sizeHint: Int = -1) extends ParserState
}
