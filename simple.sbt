name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.7"

EclipseKeys.withSource := true

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.4.1"
