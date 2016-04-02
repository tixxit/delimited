name := "delimited-core"

libraryDependencies ++= Seq(
  Deps.specs2,
  Deps.scalaCheck
)

enablePlugins(SiteScaladocPlugin)

ghpages.settings

git.remoteRepo := "git@github.com:tixxit/delimited.git"
