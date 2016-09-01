# Delimited

[![Build status](https://img.shields.io/travis/tixxit/delimited/master.svg)](https://travis-ci.org/tixxit/delimited)
[![Coverage status](https://img.shields.io/codecov/c/github/tixxit/delimited/master.svg)](https://codecov.io/github/tixxit/delimited)
[![Maven Central](https://img.shields.io/maven-central/v/net.tixxit/delimited-core_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/net.tixxit/delimited-core_2.11)

A fast, easy-to-use CSV parser for Scala.

# Set Up

Delimited is published for both Scala 2.10 and 2.11. To get started with SBT,
use the following in your SBT build file:

```scala
libraryDependencies += "net.tixxit" %% "delimited-core" % "0.8.0"
```

If you are using [Delimited's iteratee library](http://tixxit.github.io/delimited/latest/api/#net.tixxit.delimited.iteratee.Delimited$)
then you will also want to include:

```scala
libraryDependencies += "net.tixxit" %% "delimited-iteratee" % "0.8.0"
```

## [API Docs (Scaladoc)](http://tixxit.github.io/delimited/latest/api/)

The latest version of the API docs are available here: [http://tixxit.github.io/delimited/latest/api/](http://tixxit.github.io/delimited/latest/api/)

# Overview

Delimited is a library for parsing CSV and CSV-like data in Scala. The parsing
portion of the library revolves around [DelimitedParser](http://tixxit.github.io/delimited/latest/api/#net.tixxit.delimited.DelimitedParser).

```scala
import java.io.{ BufferedReader, File, FileReader }
import net.tixxit.delimited._

// Create a parser for CSVs.
val parser: DelimitedParser =
  DelimitedParser(DelimitedFormat.CSV)

// Parse a CSV string into a Vector
val rows: Vector[Either[DelimitedError, Row]] =
  parser.parseString("a,b,c\nd,e,f")

// Parse a File into a Vector
val rows: Vector[Either[DelimitedError, Row]] =
  parser.parseFile(new File("some-data.csv"))

// Parse a Reader into an Iterator
val reader: Reader = new BufferedReader(new FileReader("some-data.csv", "utf-8"))
val rows: Iterator[Either[DelimitedError, Row]] =
  parser.parseReader(reader)
```

## Fully Configurable Formats

Delimited is fairly open in how a format has been "delimited". You can specify
the following parameters for parsing/rendering:

  * separators
  * quote characters
  * quote escaping
  * row delimiters
  * are row delimiters allowed in quotes?
  * etc

For example, we may want to delimit our rows with pipes, instead of newlines.

```scala
val MyCSV: DelimitedFormat = DelimitedFormat.CSV.withRowDelim("|")
val rows: Vector[Either[DelimitedError, Row]] =
  DelimitedParser(MyCSV).parseString("a,b,c|d,e,f")
assert(rows == Vector(Right(Row("a", "b", "c")), Right(Row("d", "e", "f"))))
```

The actual choices for all the delimiters/options doesn't really matter though.
We could get really fancy, for example:

```scala
val myFormat: DelimitedFormat = DelimitedFormat(
  separator = "  ", // 2-spaces
  quote = "''", // Use 2 single quotes for quote characters
  quoteEscape = "%", // Escape quotes inside quotes with this character
  rowDelim = RowDelim("\u0000"), // Separate rows with a null character
  allowRowDelimInQuotes = true // Allow row delimiters inside quotes
)
val rows: Vector[Either[DelimitedError, Row]] =
  DelimitedParser(myFormat).parseString("a  b c  d\u0000''e \u0000 ''  ''%''''  f")
assert(rows == Vector(Right(Row("a", "b c", "d")), Right(Row("e \u0000 ", "''", "f"))))
```

## Format Inference

Delimited can infer any unspecified parameters in your `DelimitedFormat`. Only
pick what you know, even if that is nothing! This is handy when playing with
data for the first time, or when you need to parse delimited files from unknown
sources.

The easy way to get started is just to use [`DelimitedFormat.Guess`](http://tixxit.github.io/delimited/latest/api/#net.tixxit.delimited.DelimitedFormat$),
which is a [`DelimitedFormatStrategy`](http://tixxit.github.io/delimited/latest/api/#net.tixxit.delimited.DelimitedFormatStrategy)
for basic inference of the various [`DelimitedFormat`](http://tixxit.github.io/delimited/latest/api/#net.tixxit.delimited.DelimitedFormat)
parameters.

```scala
val parser: DelimitedParser = DelimitedParser(DelimitedFormat.Guess)
val rows: Vector[Either[DelimitedError, Row]] =
  parser.parseString("a\tb\tc\nd\te\tf\ng\t\"h\ti\"\tj")
assert(rows == Vector(
  Right(Row("a", "b", "c")),
  Right(Row("d", "e", "f")),
  Right(Row("g", "h\ti", "j"))))
```

You can also fix certain parameters if you know them ahead of time:

```scala
val TSVish = DelimitedFormat.Guess
  .withSeparator("\t") // tab-delimited values
  .withRowDelim("\n")  // newline delimited rows
val parser: DelimitedParser = DelimitedParser(TSVish)
val rows: Vector[Either[DelimitedError, Row]] =
  parser.parseString("a,b,c\td,e,f\ng,h,i\tj,k,l")
assert(rows == Vector(
  Right(Row("a,b,c", "d,e,f")),
  Right(Row("g,h,i", "j,k,l"))))
```

## Streaming Parser

Delimited supports streaming parsing. You feed in chunks of data and it'll give
you the rows as they are able to be parsed. For simple data import jobs, this
means you can parse a huge CSV in a constant amount of memory. Don't know the
format ahead of time? No problem! Format inference works just fine with
streaming parsing.

While things like [`parseReader`](http://tixxit.github.io/delimited/latest/api/index.html#net.tixxit.delimited.DelimitedParser@parseReader(reader:java.io.Reader):Iterator[Either[net.tixxit.delimited.DelimitedError,net.tixxit.delimited.Row]])
use the streaming parser behind the scenes, it is useful to know why this is
useful outside of this context. The important point is that `DelimitedParser`s
are actually immutable. To parse a stream of data, you create new `DelimitedParser`s
successively by feeding chunks of character data into the
[`parseChunk`](http://tixxit.github.io/delimited/latest/api/index.html#net.tixxit.delimited.DelimitedParser@parseChunk(chunk:Option[String]):(net.tixxit.delimited.DelimitedParser,Vector[Either[net.tixxit.delimited.DelimitedError,net.tixxit.delimited.Row]]))
method. This method returns any rows that were able to be parsed,as well as a
new `DelimitedParser` that can be used to parse the next chunk. This makes it
easier to integrate Delimited into functional stream processing frameworks.

```scala
val parser0: DelimitedParser = DelimitedParser(DelimitedFormat.CSV)
val (parser1, rows1) = parser0.parseChunk(Some("a,b,c\nd"))
val (parser2, rows2) = parser1.parseChunk(Some(",e,"))
val (parser3, rows3) = parser2.parseChunk(Some("f\ng,h,i\nj,k,l"))
val (parser4, rows4) = parser3.parseChunk(None)

// Only the first row is available after the first chunk has been parsed.
assert(rows1 == Vector(Right(Row("a", "b", "c"))))
// The 2nd chunk doesn't have a row delimiter, so no rows can be returned.
assert(rows2 == Vector())
// The last chunk contains the end of the 2nd row, as well as the 3rd row.
assert(rows3 == Vector(
  Right(Row("d", "e", "f")),
  Right(Row("g", "h", "i"))))
// We use None to indicate EOF, so the parser is able to return the last row.
assert(rows1 == Vector(Right(Row("a", "b", "c"))))
```

### Iteratee Support (Cats)

Delimited also has support for [Travis Brown's Iteratee](https://github.com/travisbrown/iteratee)
library for [cats](https://github.com/typelevel/cats) in the
`delimited-iteratee` library. You can 
[review the scaladoc for some more details](http://tixxit.github.io/delimited/latest/api/#net.tixxit.delimited.iteratee.Delimited$).

This provides an [`Enumeratee`](https://travisbrown.github.io/iteratee/api/index.html#io.iteratee.Enumeratee)
that parses chunks of character data into rows. It supports the same format
inference as the methods on `DelimitedParser`.

```scala

import io.iteratee._
import net.tixxit.delimited.iteratee.Delimited

// An Iteratee that renders a sequence of Rows using the specified format.
val formatAsTSV: Iteratee[Lazy, Row, String] =
  Delimited.formatString(DelimitedFormat.TSV)
// An Enumeratee that parses delimited files (guesses format).
val parseAny: Enumeratee[Lazy, String, Row] =
  Delimited.parseString(DelimitedFormat.Guess)
// An iteratee that converts a delimited file in an inferred format to a TSV.
val convert: Iteratee[Lazy, String, String] = 
  formatAsTSV.through(parseAny)
```
