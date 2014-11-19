///
 // Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 ///

import ScalaxbKeys._
import scalariform.formatter.preferences._
import AssemblyKeys._

assemblySettings

name := "scoozie"

organization := "com.klout"

version := "0.5.5"

scalaVersion := "2.10.4"

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

libraryDependencies ++= Seq(
    "org.specs2" %% "specs2-core" % "2.4.11" % "test",
    "com.google.guava" % "guava" % "18.0",
    "org.apache.oozie" % "oozie-client" % "3.3.2-cdh4.7.0",
    "org.apache.oozie" % "oozie-core" % "3.3.2-cdh4.7.0",
    "org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.7.0"
)


resolvers ++= Seq(
    "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
    "releases"  at "http://oss.sonatype.org/content/repositories/releases",
    "cloudera" at "https://repository.cloudera.com/artifactory/public"
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

packageName in scalaxb in Compile := "workflow"

scalacOptions ++= Seq(
    "-unchecked",
    "-feature",
    "-language:existentials",
    "-language:postfixOps",
    "-language:implicitConversions"
)

publishTo := Some("kloutLibraryReleases" at "http://maven-repo:8081/artifactory/libs-release-local")

credentials := Credentials(Path.userHome / ".ivy2" / ".credentials") :: Nil
