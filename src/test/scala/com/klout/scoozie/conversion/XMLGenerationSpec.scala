/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie.conversion

import com.klout.scoozie.Scoozie
import com.klout.scoozie.dsl.{ End, Job, Start, Workflow }
import com.klout.scoozie.jobs.{ JavaJob, MapReduceJob }
import oozie.workflow._
import oozie.workflow.shell.{ ACTION => SHELLACTION }
import org.specs2.mutable._

import scalaxb._

class XMLGenerationSpec extends Specification {
    "XML Generation" should {
        "given a user created job it should generate the correct workflow" in {
            case class MyShell(jobName: String = "shell-test") extends Job[SHELLACTION] {
                override val record: DataRecord[SHELLACTION] = DataRecord(None, Some("shell"), SHELLACTION(
                    jobu45tracker = None,
                    nameu45node = None,
                    prepare = None,
                    jobu45xml = Nil,
                    configuration = None,
                    exec = "echo test",
                    argument = Nil,
                    envu45var = Nil,
                    file = Nil,
                    archive = Nil,
                    captureu45output = None,
                    xmlns = "uri:oozie:shell-action:0.3"))
            }

            val firstJob = MyShell("test") dependsOn Start
            val end = End dependsOn firstJob
            val wf = Workflow("test-user-action", end)

            val expectedResult =
                """<workflow-app name="test-user-action" xmlns="uri:oozie:workflow:0.5">
          |    <start to="test"/>
          |    <action name="test">
          |        <shell xmlns="uri:oozie:shell-action:0.3">
          |            <exec>echo test</exec>
          |        </shell>
          |        <ok to="end"/>
          |        <error to="kill"/>
          |    </action>
          |    <kill name="kill">
          |        <message>test-user-action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
          |    </kill>
          |    <end name="end"/>
          |</workflow-app>""".stripMargin

            Scoozie(wf) must_== expectedResult
        }

        "give workflow with double quotes rather than &quot;" in {
            val firstJob = MapReduceJob("first") dependsOn Start
            val jsonJob = JavaJob(
                mainClass = "test.class",
                configuration = List(
                    "testJson" -> """{ "foo" : "bar" }"""
                )
            ) dependsOn firstJob
            val end = End dependsOn jsonJob
            val wf = Workflow("test-post-processing", end)

            Scoozie(wf) must_== postProcessedXml
        }
    }

    val postProcessedXml =
        """<workflow-app name="test-post-processing" xmlns="uri:oozie:workflow:0.5">
    <start to="mr_first"/>
    <action name="mr_first">
        <map-reduce>
            <job-tracker>${jobTracker}</job-tracker>
            <name-node>${nameNode}</name-node>
        </map-reduce>
        <ok to="java_class"/>
        <error to="kill"/>
    </action>
    <action name="java_class">
        <java>
            <job-tracker>${jobTracker}</job-tracker>
            <name-node>${nameNode}</name-node>
            <configuration>
                <property>
                    <name>testJson</name>
                    <value>{ "foo" : "bar" }</value>
                </property>
            </configuration>
            <main-class>test.class</main-class>
        </java>
        <ok to="end"/>
        <error to="kill"/>
    </action>
    <kill name="kill">
        <message>test-post-processing failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>"""
}