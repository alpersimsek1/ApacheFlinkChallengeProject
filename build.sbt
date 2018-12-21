name := "TChallenge"

version := "0.1"

scalaVersion := "2.11.12"

val flinkVersion = "1.7.0"

libraryDependencies ++=  Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion, // % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion //% "provided"
)


assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF")              => MergeStrategy.discard
  case _                                          => MergeStrategy.first
}

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
