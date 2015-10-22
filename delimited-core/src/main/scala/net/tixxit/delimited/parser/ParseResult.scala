package net.tixxit.delimited
package parser

sealed trait Instr[+A]

object Instr {
  sealed trait ParseResult[+A] extends Instr[A]

  case class Emit[+A](value: A) extends ParseResult[A]
  case class Fail(message: String, pos: Long) extends ParseResult[Nothing]
  case object NeedInput extends ParseResult[Nothing]

  case object Resume extends Instr[Nothing]
  case object Done extends Instr[Nothing]
}
