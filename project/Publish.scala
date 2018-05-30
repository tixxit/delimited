import sbt._
import sbt.Keys._
import com.typesafe.sbt.pgp.PgpKeys
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations._

object Publish {
  val baseSettings = Seq(
    // release stuff
    releaseCrossBuild := true,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := Function.const(false),
    publishTo := {
      val v = version.value
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("Snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("Releases" at nexus + "service/local/staging/deploy/maven2")
    },
    homepage := Some(url("http://github.com/tixxit/delimited")),
    licenses += ("ISC License", url("https://opensource.org/licenses/ISC")),
    apiURL := Some(url("https://tixxit.github.io/delimited/latest/api/")),
    autoAPIMappings := true
  )

  val settings = baseSettings ++ Seq(
    pomExtra := (
      <scm>
        <url>git@github.com:tixxit/delimited.git</url>
        <connection>scm:git:git@github.com:tixxit/delimited.git</connection>
      </scm>
      <developers>
        <developer>
          <id>tixxit</id>
          <name>Tom Switzer</name>
          <url>http://tomswitzer.net/</url>
        </developer>
      </developers>
    ),
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      releaseStepCommand("package"),
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      releaseStepCommand("publishSigned"),
      setNextVersion,
      commitNextVersion,
      releaseStepCommand("sonatypeReleaseAll"),
      pushChanges)
  )

  val skip = baseSettings ++ Seq(
    publish := {},
    publishLocal := {},
    publishArtifact := false
  )
}
