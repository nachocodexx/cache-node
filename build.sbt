
lazy val app = (project in file(".")).settings(
  name := "cache-node",
  version := "0.1",
  scalaVersion := "2.13.6",
  libraryDependencies ++= Dependencies(),
  assemblyJarName := "cachenode.jar",
  Compile / run / mainClass := Some("mx.cinvestav.Main"),
  assembly / mainClass := Some("mx.cinvestav.MainV5"),
  addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.0" cross CrossVersion.full),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
)

