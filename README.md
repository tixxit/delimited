This is a port of Framian's CSV library, but gets rid of the dependency on
Framian.

## Features

### Fully Configurable Formats

Delimited is fairly open in how a format has been "delimited". You can:

  * separators
  * quote characters
  * quote escaping
  * empty/invalid values
  * row delimiters
  * are row delimiters allowed in quotes?
  * etc

### Format Inference

Delimited can infer any unspecified parameters in your `DelimitedFormat`. Only
pick what you know. This is handy when playing with data for the first time,
or when getting delimited files from customers.

### Streaming Parser

Delimited supports streaming parsing. You feed in chunks of data and it'll give
you the rows as they are able to be parsed.

Coming soon: scalaz-stream support + more.
