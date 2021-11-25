scalaVersion := "2.13.7"
name := "Beeline"
organization := "com.beeline.scala"
version := "1.0"
scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "3.2.0")
)