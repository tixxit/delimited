import sbt._

object Deps {
  object V {
    val iteratee   = "0.19.0"
    val scalaTest = "3.1.4"
    val scalaTestPlusCheck  = "3.2.2.0"
    val scalaCheck = "1.15.2"
  }

  val iteratee   = "io.iteratee"    %% "iteratee-core" % V.iteratee
  val scalaTest  = "org.scalatest"  %% "scalatest"     % V.scalaTest  % "test"
  val scalaCheck = "org.scalacheck" %% "scalacheck"    % V.scalaCheck % "test"
  val scalaTestScalaCheck = "org.scalatestplus" %% "scalacheck-1-14"    % V.scalaTestPlusCheck % "test"
  val simpleFlatMapperCsv = "org.simpleflatmapper" % "sfm-csv" % "8.2.3"
}
