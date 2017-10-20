import Dependencies._

val libVersion = sys.env.get("TRAVIS_TAG") orElse sys.env.get("BUILD_LABEL") getOrElse s"1.0.0-${System.currentTimeMillis / 1000}-SNAPSHOT"
lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "com.indix",
      scalaVersion := "2.11.11",
      crossScalaVersions := Seq("2.10.6", "2.11.11"),
      version := libVersion
    )),
  name := "sparkplug",
  libraryDependencies ++= Seq(sparkCore, sparkSql, sparkHive, scalaTest)
)
