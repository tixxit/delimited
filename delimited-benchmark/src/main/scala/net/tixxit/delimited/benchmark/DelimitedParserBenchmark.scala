package net.tixxit.delimited
package benchmark

import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

import net.tixxit.delimited.parser._

class DelimitedParserBenchmark {
  import DelimitedParserBenchmark._

  @Benchmark
  def parseNarrowTSV(data: Data): Vector[Either[DelimitedError, Row]] = {
    DelimitedParser(data.format).parseString(data.narrowTsv)
  }

  @Benchmark
  def parseWideTSV(data: Data): Vector[Either[DelimitedError, Row]] = {
    DelimitedParser(data.format).parseString(data.wideTsv)
  }
}

object DelimitedParserBenchmark {

  @State(Scope.Benchmark)
  class Data {
    val format: DelimitedFormat = DelimitedFormat.TSV
    val narrowRow: Row = Row("aaaaaa", "aa", "aaaaaaaaaaaaaaaaaaaaaaaaa")
    val narrowTsv: String =
      List.fill(1000)(narrowRow.render(format)).mkString(format.rowDelim.value)

    val wideRow: Row = Row(List.fill(30)(narrowRow).flatMap(_.toList): _*)
    val wideTsv: String =
      List.fill(1000)(wideRow.render(format)).mkString(format.rowDelim.value)
  }
}
