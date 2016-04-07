package net.tixxit.delimited.parser

final class InputBuffer(input: Input) {
  def this(state: ParserState) = {
    this(state.input)
    setPos(state.readFrom)
  }

  private[this] var pos: Int = 0
  private[this] val chunk: String = input.data

  def getPos(): Long = input.offset + pos.toLong
  def setPos(p: Long): Unit = pos = (p.toLong - input.offset).toInt

  def getChar(): Char = chunk.charAt(pos)
  def advance(i: Int): Unit = pos += i
  def retreat(i: Int): Unit = pos -= i
  def endOfInput(): Boolean = pos >= chunk.length
  def endOfFile(): Boolean = endOfInput() && input.isLast

  def isFlag(str: String): Int = {
    def loop(i: Int): Int =
      if (i >= str.length) {
        retreat(i)
        i
      } else if (endOfInput()) {
        retreat(i)
        if (endOfFile) 0 else -1
      } else if (str.charAt(i) == getChar()) {
        advance(1)
        loop(i + 1)
      } else {
        retreat(i)
        0
      }

    loop(0)
  }

  def eitherFlag(f1: String, f2: String): Int = {
    val i = isFlag(f1)
    if (i == 0 && f2 != null) isFlag(f2) else i
  }
}
