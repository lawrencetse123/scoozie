/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie.conversion

import com.klout.scoozie.Scoozie
import com.klout.scoozie.dsl.{ End, Job, Start }
import com.klout.scoozie.jobs.{ JavaJob, MapReduceJob }
import com.klout.scoozie.workflow.WorkflowImpl
import oozie._
import org.specs2.mutable._

import scalaxb._

class XMLGenerationSpec extends Specification {
    "XML Generation" should {
        "be able to successfully generate a coordinator" in {
            import oozie.coordinator._

            val coordinator = COORDINATORu45APP(
                parameters = None,
                controls = Some(CONTROLS(Some(CONTROLSSequence1(
                    timeout = Some("10"),
                    concurrency = Some("${concurrency_level}"),
                    execution = Some("${execution_order}"),
                    throttle = Some("${materialization_throttle}")
                )))),
                datasets = Some(DATASETS(Some(DATASETSSequence1(
                    datasetsoption = Seq(
                        DataRecord(None, Some("dataset"), SYNCDATASET(
                            name = "din",
                            frequency = "${coord:endOfDays(1)}",
                            initialu45instance = "2009-01-02T08:00Z",
                            timezone = "America/Los_Angeles",
                            uriu45template = "${baseFsURI}/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}",
                            doneu45flag = None)),
                        DataRecord(None, Some("dataset"), SYNCDATASET(
                            name = "dout",
                            frequency = "${coord:minutes(30)}",
                            initialu45instance = "2009-01-02T08:00Z",
                            timezone = "UTC",
                            uriu45template = "${baseFsURI}/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}",
                            doneu45flag = None))
                    ))))),
                inputu45events = Some(INPUTEVENTS(Seq(
                    DataRecord(None, Some("data-in"), DATAIN(
                        name = "input",
                        dataset = "din",
                        datainoption = Seq(DataRecord(None, Some("instance"), "${coord:current(0)}"))
                    ))))),
                outputu45events = Some(OUTPUTEVENTS(Seq(DATAOUT(
                    name = "output",
                    dataset = "dout",
                    instance = "${coord:current(1)}"
                )))),
                inputu45logic = None,
                action = ACTION(WORKFLOW(
                    appu45path = "${wf_app_path}",
                    configuration = Some(CONFIGURATION(Seq(
                        Property2(name = "wfInput", value = "${coord:dataIn('input')}"),
                        Property2(name = "wfOutput", value = "${coord:dataOut('output')}")
                    ))))),
                frequency = "${coord:days(1)}",
                start = "2009-01-02T08:00Z",
                end = "2009-01-04T08:00Z",
                timezone = "America/Los_Angeles",
                name = "hello-coord"
            )

            val expectedResult = """<coordinator-app timezone="America/Los_Angeles" end="2009-01-04T08:00Z" start="2009-01-02T08:00Z" frequency="${coord:days(1)}" name="hello-coord" xmlns="uri:oozie:coordinator:0.4">
                                   |    <controls>
                                   |        <timeout>10</timeout>
                                   |        <concurrency>${concurrency_level}</concurrency>
                                   |        <execution>${execution_order}</execution>
                                   |        <throttle>${materialization_throttle}</throttle>
                                   |    </controls>
                                   |    <datasets>
                                   |        <dataset timezone="America/Los_Angeles" initial-instance="2009-01-02T08:00Z" frequency="${coord:endOfDays(1)}" name="din">
                                   |            <uri-template>${baseFsURI}/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}</uri-template>
                                   |        </dataset>
                                   |        <dataset timezone="UTC" initial-instance="2009-01-02T08:00Z" frequency="${coord:minutes(30)}" name="dout">
                                   |            <uri-template>${baseFsURI}/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}</uri-template>
                                   |        </dataset>
                                   |    </datasets>
                                   |    <input-events>
                                   |        <data-in dataset="din" name="input">
                                   |            <instance>${coord:current(0)}</instance>
                                   |        </data-in>
                                   |    </input-events>
                                   |    <output-events>
                                   |        <data-out dataset="dout" name="output">
                                   |            <instance>${coord:current(1)}</instance>
                                   |        </data-out>
                                   |    </output-events>
                                   |    <action>
                                   |        <workflow>
                                   |            <app-path>${wf_app_path}</app-path>
                                   |            <configuration>
                                   |                <property>
                                   |                    <name>wfInput</name>
                                   |                    <value>${coord:dataIn('input')}</value>
                                   |                </property>
                                   |                <property>
                                   |                    <name>wfOutput</name>
                                   |                    <value>${coord:dataOut('output')}</value>
                                   |                </property>
                                   |            </configuration>
                                   |        </workflow>
                                   |    </action>
                                   |</coordinator-app>""".stripMargin

            Scoozie(coordinator) must_== expectedResult
        }

        "given a user created job it should generate the correct workflow" in {
            import oozie.workflow.shell.ACTION

            case class MyShell(jobName: String = "shell-test") extends Job[ACTION] {
                override val record: DataRecord[ACTION] = DataRecord(None, Some("shell"), ACTION(
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
            val workflow = WorkflowImpl("test-user-action", end)

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

            Scoozie(workflow) must_== expectedResult
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
            val wf = WorkflowImpl("test-post-processing", end)

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