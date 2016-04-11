# Delimited

[![Build status](https://img.shields.io/travis/tixxit/delimited/master.svg)](https://travis-ci.org/tixxit/delimited)
[![Coverage status](https://img.shields.io/codecov/c/github/tixxit/delimited/master.svg)](https://codecov.io/github/tixxit/delimited)
[![Maven Central](https://img.shields.io/maven-central/v/net.tixxit/delimited-core_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/net.tixxit/delimited-core_2.11)

A fast, easy-to-use CSV parser for Scala.

# Set Up

Delimited is published for both Scala 2.10 and 2.11. To get started with SBT,
use the following in your SBT build file:

```scala
libraryDependencies += "net.tixxit" %% "delimited-core" % "0.7.0"
```

If you are using [Delimited's iteratee library](http://tixxit.github.io/delimited/latest/api/#net.tixxit.delimited.iteratee.Delimited$)
then you will also want to include:

```scala
libraryDependencies += "net.tixxit" %% "delimited-iteratee" % "0.7.0"
```

## [API Docs (Scaladoc)](http://tixxit.github.io/delimited/latest/api/)

The latest version of the API docs are available here: [http://tixxit.github.io/delimited/latest/api/](http://tixxit.github.io/delimited/latest/api/)

# Overview

## Fully Configurable Formats

Delimited is fairly open in how a format has been "delimited". You can specify
the following parameters for parsing/rendering:

  * separators
  * quote characters
  * quote escaping
  * row delimiters
  * are row delimiters allowed in quotes?
  * etc

## Format Inference

Delimited can infer any unspecified parameters in your `DelimitedFormat`. Only
pick what you know, even if that is nothing! This is handy when playing with
data for the first time, or when you need to parse delimited files from unknown
sources.

## Streaming Parser

Delimited supports streaming parsing. You feed in chunks of data and it'll give
you the rows as they are able to be parsed. For simple data import jobs, this
means you can parse a huge CSV in a constant amount of memory. Don't know the
format ahead of time? No problem! Format inference works just fine with
streaming parsing.

### Iteratee Support (Cats)

Delimited also has support for [Travis Brown's Iteratee](https://github.com/travisbrown/iteratee)
library for [cats](https://github.com/typelevel/cats) in the
`delimited-iteratee` library. You can 
[review the scaladoc for some more details](http://tixxit.github.io/delimited/latest/api/#net.tixxit.delimited.iteratee.Delimited$).
