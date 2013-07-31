/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie
package conversion

import jobs._
import dsl._
import workflow._
import scalaxb._
import org.specs2.mutable._
import runner._

class XMLGenerationSpec extends Specification {
    "XML Generation" should {

        "give workflow with double quotes rather than &quot;" in {
            val firstJob = MapReduceJob("first") dependsOn Start
            val jsonJob = JavaJob(
                mainClass = "test.class",
                configuration = List(
                    "testJson" -> """{
    	     						"foo" : "bar"
    	     						}"""
                )
            ) dependsOn firstJob
            val end = End dependsOn jsonJob
            val wf = Workflow("test-post-processing", end)

            RunWorkflow.getXMLString(wf) must_== postProcessedXml
        }
    }

    val postProcessedXml = """<workflow-app name="test-post-processing" xmlns="uri:oozie:workflow:0.2">
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