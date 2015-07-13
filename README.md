This is a port of Framian's CSV library, but gets rid of the dependency on
Framian.

## Features

### Fully Configurable Formats

Delimited is fairly open in how a format has been "delimited". You can specify
the following parameters for parsing/rendering:

  * separators
  * quote characters
  * quote escaping
  * empty/invalid values
  * row delimiters
  * are row delimiters allowed in quotes?
  * etc

### Format Inference

Delimited can infer any unspecified parameters in your `DelimitedFormat`. Only
pick what you know. This is handy when playing with data for the first time, or
when you need to parse delimited files from unknown sources.

### Streaming Parser

Delimited supports streaming parsing. You feed in chunks of data and it'll give
you the rows as they are able to be parsed. For simple data import jobs, this
means you can parse a huge CSV in a constant amount of memory.

Coming soon: scalaz-stream support + more.
