name := "delimited-core"

libraryDependencies ++= Seq(
  Deps.scalaTest,
  Deps.scalaCheck
)

enablePlugins(SiteScaladocPlugin)

ghpages.settings

git.remoteRepo := "git@github.com:tixxit/delimited.git"
