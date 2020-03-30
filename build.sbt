organization in ThisBuild := "net.tixxit"

licenses in ThisBuild += ("BSD-style" -> url("http://opensource.org/licenses/MIT"))

scalaVersion in ThisBuild := "2.13.1"

crossScalaVersions in ThisBuild := Seq("2.11.12", "2.12.11", "2.13.1")

scalacOptions in ThisBuild ++= Seq("-deprecation", "-feature", "-unchecked", "-language:higherKinds")

maxErrors in ThisBuild := 5

addCommandAlias("publishDocs", ";unidoc;ghpagesPushSite")

import com.typesafe.sbt.site.SitePlugin.autoImport._

lazy val root = project.
  in(file(".")).
  aggregate(delimitedCore, delimitedIteratee).
  enablePlugins(GhpagesPlugin, ScalaUnidocPlugin, SiteScaladocPlugin).
  settings(Publish.skip: _*).
  settings(
    name := "delimited",
    addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in SiteScaladoc),
    autoAPIMappings := true,
    git.remoteRepo := "git@github.com:tixxit/delimited.git"
  )

lazy val delimitedCore = project.
  in(file("delimited-core")).
  settings(Publish.settings: _*)

lazy val delimitedIteratee = project.
  in(file("delimited-iteratee")).
  dependsOn(delimitedCore).
  settings(Publish.settings: _*)

lazy val delimitedBenchmark = project.
  in(file("delimited-benchmark")).
  dependsOn(delimitedCore).
  settings(Publish.skip: _*)
