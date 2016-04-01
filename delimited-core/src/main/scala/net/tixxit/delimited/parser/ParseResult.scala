package net.tixxit.delimited
package parser

sealed trait Instr
sealed trait ParseResult

object Instr {

  case object Success extends ParseResult

  case class Fail(message: String, pos: Long) extends ParseResult with Instr
  case object NeedInput extends ParseResult with Instr

  case class EmitRow(row: Row) extends Instr
  case object Resume extends Instr
  case object Done extends Instr
}
