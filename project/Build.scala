import sbt._
import Keys._
import com.typesafe.startscript.StartScriptPlugin

object BuildSettings {
    import Dependencies._
    import Resolvers._

    val buildOrganization = "com.ometer"
    val buildVersion = "1.0"
    val buildScalaVersion = "2.9.0-1"

    val globalSettings = Seq(
        organization := buildOrganization,
        version := buildVersion,
        scalaVersion := buildScalaVersion,
        scalacOptions ++= Seq("-deprecation", "-unchecked"),
        fork in test := true,
        fork in run := true,
        libraryDependencies ++= Seq(slf4jSimpleTest, scalatest),
        resolvers := Seq(scalaToolsRepo, akkaRepo))

    val projectSettings = Defaults.defaultSettings ++ globalSettings
}

object Resolvers {
    val scalaToolsRepo = "Scala Tools" at "http://scala-tools.org/repo-snapshots/"
    val akkaRepo = "Akka" at "http://akka.io/repository/"
}

object Dependencies {
    val scalatest = "org.scalatest" %% "scalatest" % "1.6.1" % "test"
    val slf4jSimple = "org.slf4j" % "slf4j-simple" % "1.6.2"
    val slf4jSimpleTest = slf4jSimple % "test"

    val akka = "se.scalablesolutions.akka" % "akka-actor" % "1.2"
}

object ExpirationBuild extends Build {
    import BuildSettings._
    import Dependencies._
    import Resolvers._

    override lazy val settings = super.settings ++ globalSettings

    lazy val root = Project("expiration",
                            file("."),
                            settings = projectSettings ++
                            StartScriptPlugin.startScriptForClassesSettings ++
                            Seq(libraryDependencies ++= Seq(akka)))
}

