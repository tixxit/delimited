resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

addSbtPlugin("com.eed3si9n"       % "sbt-unidoc"      % "0.4.1")
addSbtPlugin("com.github.gseitz"  % "sbt-release"     % "1.0.8")
addSbtPlugin("com.jsuereth"       % "sbt-pgp"         % "1.1.1")
addSbtPlugin("com.typesafe"       % "sbt-mima-plugin" % "0.7.0")
addSbtPlugin("com.typesafe.sbt"   % "sbt-ghpages"     % "0.6.2")
addSbtPlugin("com.typesafe.sbt"   % "sbt-site"        % "1.3.2")
addSbtPlugin("org.scoverage"      % "sbt-scoverage"   % "1.6.1")
addSbtPlugin("org.xerial.sbt"     % "sbt-sonatype"    % "2.3")
addSbtPlugin("pl.project13.scala" % "sbt-jmh"         % "0.3.4")
