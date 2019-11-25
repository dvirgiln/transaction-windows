organization := "com.dvirgil"
version := "0.1-SNAPSHOT"
name := "transaction-windows"
scalaVersion := "2.12.8"

val commonDependencies: Seq[ModuleID] = Seq(
  "org.scalatest" %% "scalatest" % "3.0.1"
)

val root = (project in file(".")).
  settings(
    libraryDependencies ++= commonDependencies,
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-language:_"
    )
  )
