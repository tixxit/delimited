import sbt._

object Deps {
  object V {
    val iteratee   = "0.12.0"
    val scalaTest  = "3.0.3"
    val scalaCheck = "1.13.5"
  }

  val iteratee   = "io.iteratee"    %% "iteratee-core" % V.iteratee
  val scalaTest  = "org.scalatest"  %% "scalatest"     % V.scalaTest  % "test"
  val scalaCheck = "org.scalacheck" %% "scalacheck"    % V.scalaCheck % "test"
}
