package net.tixxit.delimited.parser

import scala.annotation.tailrec

final class InputBuffer(input: Input) {
  def this(state: ParserState) = {
    this(state.input)
    setPos(state.readFrom)
  }

  private[this] var pos: Int = 0
  private[this] val chunk: String = input.data
  private[this] val clen = chunk.length

  def getPos(): Long = input.offset + pos.toLong
  def setPos(p: Long): Unit = pos = (p.toLong - input.offset).toInt

  def getChar(): Char = chunk.charAt(pos)
  def advance(i: Int): Unit = pos += i
  def retreat(i: Int): Unit = pos -= i
  def endOfInput(): Boolean = pos >= clen
  def endOfFile(): Boolean = endOfInput() && input.isLast

  def isFlag(str: String): Int = {
    val slen = str.length

    // there are a few cases:
    // 1. clen >= slen
    //   a. chunk starts with str: return slen
    //   b. return 0
    // 2. clen < slen
    //   a. if str.startsWith(chunk)
    //     i. input.isLast return 0
    //     ii. return -1
    //   b. return 0

    /*
     * This code is equivalent, but seems to benchmark worse
     *
    if ((clen - pos) >= slen) {
      if (chunk.startsWith(str, pos)) slen
      else 0
    }
    else {
      if (str.startsWith(chunk.substring(pos))) {
        if (input.isLast) 0 else -1
      }
      else 0
    }
    */

    var p = pos
    var i = 0
    var res = -2
    while (res == -2) {
      if (i >= slen) {
        res = i
      }
      else if (p >= clen) {
        // we exhausted this chunk
        res = if (input.isLast) 0 else -1
      }
      else if (str.charAt(i) != chunk.charAt(p)) {
        res = 0
      }

      i += 1
      p += 1
    }
    res
  }

  def eitherFlag(f1: String, f2: String): Int = {
    val i = isFlag(f1)
    if (i == 0 && (f2 ne null)) isFlag(f2) else i
  }
}
