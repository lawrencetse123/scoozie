///
// Copyright (C) 2013 Klout Inc. <http://www.klout.com>
///

import ScalaxbKeys._
import sbtrelease._
import ReleaseStateTransformations._
import ReleasePlugin.autoImport._
import scalariform.formatter.preferences._
import AssemblyKeys._

assemblySettings

name := "scoozie"

organization := "com.klout"

scalaVersion := "2.11.7"

val oozieVersion = "4.2.0"

val hadoopVersion = "2.5.2"

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2-core" % "3.6.6" % "test",
  "org.apache.oozie" % "oozie-client" % oozieVersion,
  "org.apache.oozie" % "oozie-core" % oozieVersion,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
  "com.google.guava" % "guava" % "19.0",
  "org.scala-lang.modules" %% "scala-xml" % "1.0.5",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
  "joda-time" % "joda-time" % "2.9.2"
)

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases",
  Resolver.typesafeRepo("releases"),
  Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
)

scalariformSettings

ScalariformKeys.preferences := FormattingPreferences().
  setPreference(AlignParameters, true).
  setPreference(IndentSpaces, 2).
  setPreference(AlignSingleLineCaseStatements, true).
  setPreference(PreserveDanglingCloseParenthesis, true).
  setPreference(PreserveSpaceBeforeArguments, true)

scalaxbSettings

sourceGenerators in Compile <+= scalaxb in Compile

protocolPackageName in scalaxb in Compile := Some("oozie")

packageName in scalaxb in Compile := "oozie"

contentsSizeLimit in (Compile, scalaxb) := 20

namedAttributes in(Compile, scalaxb) := true

val rootFolder = "oozie"

packageNames in scalaxb in Compile := Map(
  uri("uri:oozie:workflow:0.5") -> s"$rootFolder.workflow_0_5",
  uri("uri:oozie:hive-action:0.5") -> s"$rootFolder.hive_0_5",
  uri("uri:oozie:shell-action:0.3") -> s"$rootFolder.shell_0_3",
  uri("uri:oozie:distcp-action:0.2") -> s"$rootFolder.distcp_0_2",
  uri("uri:oozie:email-action:0.2") -> s"$rootFolder.email_0_2",
  uri("uri:oozie:sla:0.2") -> s"$rootFolder.sla_0_2",
  uri("uri:oozie:spark-action:0.1") -> s"$rootFolder.spark_0_1",
  uri("uri:oozie:sqoop-action:0.4") -> s"$rootFolder.sqoop_0_4",
  uri("uri:oozie:ssh-action:0.2") -> s"$rootFolder.ssh_0_2",
  uri("uri:oozie:coordinator:0.4") -> s"$rootFolder.coordinator_0_4",
  uri("uri:oozie:bundle:0.2") -> s"$rootFolder.bundle_0_2"
)

scalacOptions ++= Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:postfixOps",
  "-language:implicitConversions"
)

releaseProcess := Seq[ReleaseStep](
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepTask(assembly),
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

publishTo in ThisBuild := {
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some(Resolver.file("file",  new File( "maven-repo/snapshots" )) )
  else
    Some(Resolver.file("file",  new File( "maven-repo/releases" )) )
}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("META-INF", xs @ _*) =>
      (xs map {_.toLowerCase}) match {
        case "services" :: xs => MergeStrategy.filterDistinctLines
        case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) => MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.discard
      }
    case _ => MergeStrategy.first
  }
}
