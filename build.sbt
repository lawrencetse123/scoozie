///
// Copyright (C) 2013 Klout Inc. <http://www.klout.com>
///

import ScalaxbKeys._
import scalariform.formatter.preferences._
import AssemblyKeys._

assemblySettings

name := "scoozie"

organization := "com.klout"

version := "0.5.6"

scalaVersion := "2.11.7"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case PathList("META-INF", xs@_*) =>
    (xs map {
      _.toLowerCase
    }) match {
      case "services" :: xs => MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) => MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.discard
    }
  case _ => MergeStrategy.first
}
}

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2-core" % "3.6.6" % "test",
  "com.google.guava" % "guava" % "19.0",
  "org.scala-lang.modules" %% "scala-xml" % "1.0.5",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
  "org.scalaxb" % "scalaxb_2.11" % "1.4.0"
)

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases"
)

scalariformSettings

ScalariformKeys.preferences := FormattingPreferences().
  setPreference(AlignParameters, true).
  setPreference(IndentSpaces, 4).
  setPreference(AlignSingleLineCaseStatements, true).
  setPreference(PreserveDanglingCloseParenthesis, true).
  setPreference(PreserveSpaceBeforeArguments, true)

scalaxbSettings

sourceGenerators in Compile <+= scalaxb in Compile

protocolPackageName in scalaxb in Compile := Some("oozie.workflow")

packageName in scalaxb in Compile := "oozie"

contentsSizeLimit in (Compile, scalaxb) := 20

namedAttributes in(Compile, scalaxb) := true

val workflowRoot = "oozie.workflow"

packageNames in scalaxb in Compile := Map(
  uri("uri:oozie:workflow:0.2") -> workflowRoot,
  uri("uri:oozie:workflow:0.5") -> workflowRoot,
  uri("uri:oozie:hive-action:0.6") -> s"$workflowRoot.hive",
  uri("uri:oozie:shell-action:0.3") -> s"$workflowRoot.shell",
  uri("uri:oozie:distcp-action:0.2") -> s"$workflowRoot.distcp",
  uri("uri:oozie:email-action:0.2") -> s"$workflowRoot.email",
  uri("uri:oozie:sla:0.2") -> s"$workflowRoot.sla",
  uri("uri:oozie:spark-action:0.1") -> s"$workflowRoot.spark",
  uri("uri:oozie:sqoop-action:0.4") -> s"$workflowRoot.sqoop",
  uri("uri:oozie:ssh-action:0.2") -> s"$workflowRoot.ssh",
  uri("uri:oozie:coordinator:0.5") -> s"oozie.coordinator",
  uri("uri:oozie:bundle:0.2") -> s"oozie.bundle"
)

scalacOptions ++= Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:postfixOps",
  "-language:implicitConversions"
)

publishTo := Some("kloutLibraryReleases" at "http://maven-repo:8081/artifactory/libs-release-local")

credentials := Credentials(Path.userHome / ".ivy2" / ".credentials") :: Nil




