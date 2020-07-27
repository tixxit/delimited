package net.tixxit.delimited
package benchmark

import java.io.StringReader
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}
import org.simpleflatmapper.csv.CsvParser

import net.tixxit.delimited.parser._

class DelimitedParserBenchmark {
  import DelimitedParserBenchmark._

  @Benchmark
  def parseNarrowCSV(data: Data): Vector[Either[DelimitedError, Row]] = {
    DelimitedParser(data.format).parseString(data.narrowCsv)
  }

  @Benchmark
  def parseWideCSV(data: Data): Vector[Either[DelimitedError, Row]] = {
    DelimitedParser(data.format).parseString(data.wideCsv)
  }

  @Benchmark
  def parseNarrowCSVSimpleFlatMapper(data: Data): Vector[Either[DelimitedError, Row]] = {
    val iter = CsvParser.iterator(new StringReader(data.narrowCsv))
    val bldr = Vector.newBuilder[Either[DelimitedError, Row]]
    while (iter.hasNext()) {
      bldr += Right(Row.fromArray(iter.next()))
    }
    bldr.result()
  }

  @Benchmark
  def parseWideCSVSimpleFlatMapper(data: Data): Vector[Either[DelimitedError, Row]] = {
    val iter = CsvParser.iterator(new StringReader(data.wideCsv))
    val bldr = Vector.newBuilder[Either[DelimitedError, Row]]
    while (iter.hasNext()) {
      bldr += Right(Row.fromArray(iter.next()))
    }
    bldr.result()
  }
}

object DelimitedParserBenchmark {

  @State(Scope.Benchmark)
  class Data {
    val format: DelimitedFormat = DelimitedFormat.CSV
    val narrowRow: Row = Row("aaaaaa", "aa", "aaaaaaaaaaaaaaaaaaaaaaaaa")
    val narrowCsv: String =
      List.fill(1000)(narrowRow.render(format)).mkString(format.rowDelim.value)

    val wideRow: Row = Row(List.fill(30)(narrowRow).flatMap(_.toList): _*)
    val wideCsv: String =
      List.fill(1000)(wideRow.render(format)).mkString(format.rowDelim.value)
  }
}
