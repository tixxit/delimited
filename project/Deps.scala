import sbt._

object Deps {
  object V {
    val iteratee   = "0.3.1"
    val scalaTest  = "2.2.6"
    val scalaCheck = "1.12.5"
  }

  val iteratee   = "io.iteratee"    %% "iteratee-core" % V.iteratee
  val scalaTest  = "org.scalatest"  %% "scalatest"     % V.scalaTest  % "test"
  val scalaCheck = "org.scalacheck" %% "scalacheck"    % V.scalaCheck % "test"
}
