import sbt._
import sbt.Keys._
import com.typesafe.sbt.pgp.PgpKeys
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations._

object Publish {

  val settings = Seq(
    // release stuff
    releaseCrossBuild := true,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := Function.const(false),
    publishTo <<= (version).apply { v =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("Snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("Releases" at nexus + "service/local/staging/deploy/maven2")
    },
    homepage := Some(url("http://github.com/tixxit/delimited")),
    licenses += ("ISC License", url("https://opensource.org/licenses/ISC")),
    pomExtra := (
      <scm>
        <url>git@github.com:tixxit/delimited.git</url>
        <connection>scm:git:git@github.com:tixxit/delimited.git</connection>
      </scm>
      <developers>
        <developer>
          <id>tixxit</id>
          <name>Tom Switzer</name>
          <url>http://tomswitzer.net//</url>
        </developer>
      </developers>
    ),
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      ReleaseStep(action = Command.process("package", _)),
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      ReleaseStep(action = Command.process("publishSigned", _)),
      setNextVersion,
      commitNextVersion,
      ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
      pushChanges)
  )

  val skip = Seq(publish := {}, publishLocal := {}, publishArtifact := false)
}
