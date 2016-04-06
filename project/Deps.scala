import sbt._

object Deps {
  object V {
    val scalaTest  = "2.2.6"
    val scalaCheck = "1.12.5"
  }

  val scalaTest  = "org.scalatest"  %% "scalatest"  % V.scalaTest  % "test"
  val scalaCheck = "org.scalacheck" %% "scalacheck" % V.scalaCheck % "test"
}
