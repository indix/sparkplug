import Dependencies._

val libVersion = sys.env.get("TRAVIS_TAG") orElse sys.env.get("BUILD_LABEL") getOrElse s"1.0.0-${System.currentTimeMillis / 1000}-SNAPSHOT"

lazy val publishSettings = Seq(
  publishMavenStyle := true,
  pgpSecretRing := file("local.secring.gpg"),
  pgpPublicRing := file("local.pubring.gpg"),
  pgpPassphrase := Some(sys.env.getOrElse("GPG_PASSPHRASE", "").toCharArray),
  credentials += Credentials("Sonatype Nexus Repository Manager",
                             "oss.sonatype.org",
                             System.getenv("SONATYPE_USERNAME"),
                             System.getenv("SONATYPE_PASSWORD")),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := false,
  pomIncludeRepository := { _ =>
    false
  },
  pomExtra :=
    <url>https://github.com/indix/sparkplug</url>
      <licenses>
        <license>
          <name>Apache License</name>
          <url>https://raw.githubusercontent.com/indix/sparkplug/master/LICENSE</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:indix/sparkplug.git</url>
        <connection>scm:git:git@github.com:indix/sparkplug.git</connection>
      </scm>
      <developers>
        <developer>
          <id>indix</id>
          <name>Indix</name>
          <url>http://www.indix.com</url>
        </developer>
      </developers>
)

lazy val sparkplug = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "com.indix",
        scalaVersion := "2.11.11",
        crossScalaVersions := Seq("2.10.6", "2.11.11"),
        version := libVersion,
        scalafmtOnCompile := true
      )),
    name := "sparkplug",
    libraryDependencies ++= Seq(sparkCore, sparkSql, sparkHive, scalaTest)
  )
  .settings(publishSettings: _*)
