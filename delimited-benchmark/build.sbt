name := "delimited-benchmark"

enablePlugins(JmhPlugin)

libraryDependencies ++= Seq(
  Deps.simpleFlatMapperCsv
)
