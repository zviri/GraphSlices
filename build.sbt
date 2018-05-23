
lazy val root = (project in file(".")).
  settings(
    name := "GraphSlices",
    organization := "org.zviri.graphslices",
    version := "1.0",
    scalaVersion := "2.12.4"
  )

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.scalactic" %% "scalactic" % "3.0.4",
  "joda-time" % "joda-time" % "2.9.9",
  "com.typesafe.play" %% "play-json" % "2.6.9"
)

assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}
