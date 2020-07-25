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

  // advance while characters are > the given character
  def advanceGT(c: Char): Long  = {
    while (pos < clen && (chunk.charAt(pos) > c)) {
      pos += 1
    }
    pos.toLong + input.offset
  }

  def retreat(i: Int): Unit = pos -= i
  def endOfInput(): Boolean = pos >= clen
  def endOfFile(): Boolean = endOfInput() && input.isLast

  def isFlag(str: String): Int =
    isFlag(str, str.length)

  def isFlag(str: String, slen: Int): Int = {

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

    if (slen == 1) {
      // this is a common case
      if (pos >= clen) {
        // we exhausted this chunk
        if (input.isLast) 0 else -1
      }
      else if (str.charAt(0) != chunk.charAt(pos)) {
        0
      }
      else 1
    }
    else {
      if (clen - pos >= slen) {
        // the maximum i can be is slen,
        // so if clen - pos >= slen, we can't exhaust
        if (chunk.startsWith(str, pos)) slen
        else 0
      }
      else {
        var p = pos
        var i = 0
        // we know that clen - pos < slen
        // so we cannot contain the entire string
        // but we could have the beggining of str
        // and then run out of input
        // we can exhaust the input
        while (true) {
          if (p >= clen) {
            // we exhausted this chunk
            return if (input.isLast) 0 else -1
          }
          else if (str.charAt(i) != chunk.charAt(p)) {
            return 0
          }
          else {
            i += 1
            p += 1
          }
        }
        // unreachable
        Int.MinValue
      }
    }
  }

  def eitherFlag(f1: String, f2: String): Int = {
    val i = isFlag(f1)
    if (i == 0 && (f2 ne null)) isFlag(f2) else i
  }

  def eitherFlag(f1: String, f1Len: Int, f2: String, f2Len: Int): Int = {
    val i = isFlag(f1, f1Len)
    if (i == 0 && (f2 ne null)) isFlag(f2, f2Len) else i
  }
}
