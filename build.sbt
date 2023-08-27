(ThisBuild / organization) := "net.tixxit"

(ThisBuild / licenses) += ("BSD-style" -> url("http://opensource.org/licenses/MIT"))

(ThisBuild / scalaVersion) := "2.13.1"

(ThisBuild / crossScalaVersions) := Seq("2.11.12", "2.12.11", "2.13.1")

(ThisBuild / scalacOptions) ++= Seq("-deprecation", "-feature", "-unchecked", "-language:higherKinds")

(ThisBuild / maxErrors) := 5

addCommandAlias("publishDocs", ";unidoc;ghpagesPushSite")

import com.typesafe.sbt.site.SitePlugin.autoImport._

lazy val root = project.
  in(file(".")).
  aggregate(delimitedCore, delimitedIteratee).
  enablePlugins(GhpagesPlugin, ScalaUnidocPlugin, SiteScaladocPlugin).
  settings(Publish.skip: _*).
  settings(
    name := "delimited",
    addMappingsToSiteDir((ScalaUnidoc / packageDoc / mappings), (SiteScaladoc / siteSubdirName)),
    autoAPIMappings := true,
    git.remoteRepo := "git@github.com:tixxit/delimited.git"
  )

lazy val mimaVersions = Set("0.10.0", "0.10.1")

lazy val delimitedCore = project.
  in(file("delimited-core")).
  settings(Publish.settings: _*).
  settings(
    mimaPreviousArtifacts := mimaVersions.map("net.tixxit" %% "delimited-core" % _)
  )

lazy val delimitedIteratee = project.
  in(file("delimited-iteratee")).
  dependsOn(delimitedCore).
  settings(Publish.settings: _*).
  settings(
    mimaPreviousArtifacts := mimaVersions.map("net.tixxit" %% "delimited-iteratee" % _)
  )

lazy val delimitedBenchmark = project.
  in(file("delimited-benchmark")).
  dependsOn(delimitedCore).
  settings(Publish.skip: _*).
  settings(
    mimaPreviousArtifacts := Set.empty
  )
