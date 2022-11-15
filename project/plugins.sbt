resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

addSbtPlugin("com.eed3si9n"       % "sbt-unidoc"      % "0.4.3")
addSbtPlugin("com.github.gseitz"  % "sbt-release"     % "1.0.13")
addSbtPlugin("com.jsuereth"       % "sbt-pgp"         % "1.1.2-1")
addSbtPlugin("com.typesafe"       % "sbt-mima-plugin" % "0.8.1")
addSbtPlugin("com.typesafe.sbt"   % "sbt-ghpages"     % "0.6.3")
addSbtPlugin("com.typesafe.sbt"   % "sbt-site"        % "1.4.0")
addSbtPlugin("org.scoverage"      % "sbt-scoverage"   % "2.0.6")
addSbtPlugin("org.xerial.sbt"     % "sbt-sonatype"    % "3.9.4")
addSbtPlugin("pl.project13.scala" % "sbt-jmh"         % "0.4.0")

