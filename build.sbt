scalaVersion := "2.12.15"
name := "Beeline"
organization := "com.beeline.scala"
version := "1.0"
scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.lihaoyi" %% "upickle" % "0.7.1"
)