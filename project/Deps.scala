import sbt._

object Deps {
  object V {
    val Specs2             = "2.4.2"
    val ScalaCheck         = "1.11.6"
  }

  val specs2          =   "org.specs2"            %% "specs2"                      % V.Specs2        % "test"
  val scalaCheck      =   "org.scalacheck"        %% "scalacheck"                  % V.ScalaCheck    % "test"
}
