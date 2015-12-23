package net.tixxit.delimited
//package schema

//case class ColumnStats(
//)
//
//DelimitedStream
//
//sealed trait ColumnSchema
//object ColumnSchema {
//  sealed trait ColumnType {
//    type Stats
//  }
//
//  case object Numeric extends ColumnType
//  case object Text extends ColumnType
//  case object Boolean extends ColumnType
//
//  case class Numeric(min: BigDecimal, max: BigDecimal, integral: Boolean) extends ColumnSchema
//  case class Text(uniqueRatio: Double) extends ColumnSchema
//  case class Boolean(trues: Set[String], falses: Set[String]) extends ColumnSchema
//}
//
//val scoreDelimited = Delimited
//  .batched(1000, nThreads = 4)
//  .flatMap { row =>
//    Delimited.append { row =>
//      model.score(row).toJson.compact
//    }
//  }
//
//scoreDelimited.runFile(path)

trait Delimited2[I, O] {
  import Delimited2.{ Input, Chunk, Done }

  def foldWith[B](
    onCont: (Input[I] => Delimited2[I, O]) => B,
    onDone: (O, Input[I]) => B
  ): B

  def map[O2](f: O => O2): Delimited2[I, O2] =
    foldWith(
      { k => Delimited2.cont(in => k(in).map(f)) },
      { (o, in) => Delimited2.done(f(o), in) }
    )

  //def flatMap[O2](f: A => O2): Delimited2[I, O2] = {
  //  foldWith(
  //    { k =>
  //      Delimited2.cont(in => 
  //}

  def process(input: Input[I]): Delimited2[I, O] =
    foldWith(
      { k => k(input) },
      { (o, in) => Delimited2.done(o, in) }
    )

  def processChunk(is: I*): Delimited2[I, O] =
    process(Chunk(is.toVector))

  def run(): O =
    process(Done).foldWith(
      { k => k(Done).run() },
      { (o, in) => o }
    )
}

object Delimited2 {
  sealed trait Input[+A]
  case class El[+A](value: A) extends Input[A]
  case class Chunk[+A](values: Vector[A]) extends Input[A]
  case object Done extends Input[Nothing]

  def cont[I, O](f: Input[I] => Delimited2[I, O]): Delimited2[I, O] =
    new Delimited2[I, O] {
      def foldWith[B](
        onCont: (Input[I] => Delimited2[I, O]) => B,
        onDone: (O, Input[I]) => B
      ): B = onCont(f)
    }

  def done[I, O](value: O, remaining: Input[I]): Delimited2[I, O] =
    new Delimited2[I, O] {
      def foldWith[B](
        onCont: (Input[I] => Delimited2[I, O]) => B,
        onDone: (O, Input[I]) => B
      ): B = onDone(value, remaining)
    }

  def consume[A]: Delimited2[A, Vector[A]] = {
    def loop(acc: Vector[A]): Input[A] => Delimited2[A, Vector[A]] = {
      case El(a) => cont(loop(acc :+ a))
      case Chunk(as) => cont(loop(acc ++ as))
      case Done => done(acc, Done)
    }

    cont(loop(Vector.empty))
  }

  def fold[I, O](init: O)(f: (O, I) => O): Delimited2[I, O] = {
    def loop(acc: O): Input[I] => Delimited2[I, O] = {
      case El(a) => cont(loop(f(acc, a)))
      case Chunk(as) => cont(loop(as.foldLeft(acc)(f)))
      case Done => done(acc, Done)
    }

    cont(loop(init))
  }

  // def batched(size: Int, nThreads: Int): 
}
