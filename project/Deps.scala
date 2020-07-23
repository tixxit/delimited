import sbt._

object Deps {
  object V {
    val iteratee   = "0.19.0"
    val scalaTest = "3.1.2"
    val scalaTestPlusCheck  = "3.1.0.1"
    val scalaCheck = "1.14.3"
  }

  val iteratee   = "io.iteratee"    %% "iteratee-core" % V.iteratee
  val scalaTest  = "org.scalatest"  %% "scalatest"     % V.scalaTest  % "test"
  val scalaCheck = "org.scalacheck" %% "scalacheck"    % V.scalaCheck % "test"
  val scalaTestScalaCheck = "org.scalatestplus" %% "scalacheck-1-14"    % V.scalaTestPlusCheck % "test"
}
