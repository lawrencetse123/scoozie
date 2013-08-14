/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie
package verification

import jobs._
import dsl._
import scalaxb._
import org.specs2.mutable._
import runner._
import workflow._

class XMLVerificationSpec extends Specification {
    "XMLVerification" should {
        "give true for exactly the same workflows" in {
            XMLVerification.verify(simpleWf, simpleWf) must_== true
        }

        "give true for workflows with different node names" in {
            XMLVerification.verify(simpleWf, renamedSimpleWf) must_== true
        }

        "give true for hive jobs with different names" in {
            XMLVerification.verify(simpleHiveWf, renamedSimpleHiveWf) must_== true
        }

        "give false for different workflows" in {
            XMLVerification.verify(simpleWf, simpleHiveWf) must_== false
        }

        "give true for workflows with decisions" in {
            XMLVerification.verify(simpleDecisionWf, simpleDecisionWf) must_== true
        }

        "give false for decision with cases in different order" in {
            XMLVerification.verify(multipleDecisionCaseWf, reOrderedDecisionCaseWf) must_== false
        }

        "give false for decision with same paths different predicates" in {
            XMLVerification.verify(multipleDecisionCaseWf, multipleDecisionCaseWf2) must_== false
        }

        "give true for different-ordered fork paths" in {
            XMLVerification.verify(forkWf, reOrderedForkWf) must_== true
        }

        "give false for same-named fork transitions, but different paths" in {
            XMLVerification.verify(forkWf, differentForkWf) must_== false
        }
    }

    val simpleWf = """
        <workflow-app name="test" xmlns="uri:oozie:workflow:0.2">
            <start to="java_job"/>
            <action name="java_job">
                <java>
                    <job-tracker>${jobTracker}</job-tracker>
                    <name-node>${nameNode}</name-node>
                    <prepare>
                        <delete path="${nameNode}/target_path/${dateString}"/>
                    </prepare>
                    <main-class>something.class</main-class>
                    <arg>${nameNode}/source_path/${dateString}</arg>
                    <arg>${nameNode}/target_path/${dateString}</arg>
                </java>
                <ok to="java_job_2"/>
                <error to="kill"/>
            </action>
            <action name="java_job_2">
                <java>
                    <job-tracker>${jobTracker}</job-tracker>
                    <name-node>${nameNode}</name-node>
                    <main-class>somethingElse.class</main-class>
                    <arg>${nameNode}/path/${dateString}</arg>
                </java>
                <ok to="end"/>
                <error to="kill"/>
            </action>
            <kill name="kill">
                <message>test failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
            </kill>
            <end name="end"/>
        </workflow-app>
        """

    val renamedSimpleWf = """
        <workflow-app name="test" xmlns="uri:oozie:workflow:0.2">
            <start to="first job"/>
            <action name="first job">
                <java>
                    <job-tracker>${jobTracker}</job-tracker>
                    <name-node>${nameNode}</name-node>
                    <prepare>
                        <delete path="${nameNode}/target_path/${dateString}"/>
                    </prepare>
                    <main-class>something.class</main-class>
                    <arg>${nameNode}/source_path/${dateString}</arg>
                    <arg>${nameNode}/target_path/${dateString}</arg>
                </java>
                <ok to="second job"/>
                <error to="kill"/>
            </action>
            <action name="second job">
                <java>
                    <job-tracker>${jobTracker}</job-tracker>
                    <name-node>${nameNode}</name-node>
                    <main-class>somethingElse.class</main-class>
                    <arg>${nameNode}/path/${dateString}</arg>
                </java>
                <ok to="end"/>
                <error to="kill"/>
            </action>
            <kill name="kill">
                <message>test failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
            </kill>
            <end name="end"/>
        </workflow-app>
        """

    val simpleHiveWf = """
        <workflow-app name="test" xmlns="uri:oozie:workflow:0.2">
        <start to="hive_job"/>
        <action name="hive_job">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script.hql</script>
                <param>dateString=${dateString}</param>
                <param>yesterdayString=${yesterdayString}</param>
                <param>outputDataRoot=${outputDataRoot}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <kill name="kill">
            <message>test failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
        </kill>
        <end name="end"/>
        </workflow-app>
        """

    val renamedSimpleHiveWf = """
        <workflow-app name="test" xmlns="uri:oozie:workflow:0.2">
        <start to="first hive job"/>
        <action name="first hive job">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script.hql</script>
                <param>dateString=${dateString}</param>
                <param>yesterdayString=${yesterdayString}</param>
                <param>outputDataRoot=${outputDataRoot}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <kill name="kill">
            <message>test failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
        </kill>
        <end name="end"/>
        </workflow-app>
        """

    val simpleDecisionWf = """
    <workflow-app name="test" xmlns="uri:oozie:workflow:0.2">
        <start to="decision"/>
        <decision name="decision">
            <switch>
                <case to="hive_node">${doHive}</case>
                <default to="end"/>
            </switch>
        </decision>
        <action name="hive_node">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script.hql</script>
                <param>dateString=${dateString}</param>
                <param>param1=${param1}</param>
                <param>param2=${param2}</param>
                <param>param3=${param3}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <kill name="kill">
            <message>test failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
        </kill>
        <end name="end"/>
    </workflow-app>
    """

    val multipleDecisionCaseWf = """
    <workflow-app name="test" xmlns="uri:oozie:workflow:0.2">
        <start to="decision"/>
        <decision name="decision">
            <switch>
                <case to="hive_node">${doHive}</case>
                <case to="hive_node2">${doHive2}</case>
                <default to="end"/>
            </switch>
        </decision>
        <action name="hive_node">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script.hql</script>
                <param>dateString=${dateString}</param>
                <param>param1=${param1}</param>
                <param>param2=${param2}</param>
                <param>param3=${param3}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <action name="hive_node2">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script2.hql</script>
                <param>dateString=${dateString}</param>
                <param>param1=${param1}</param>
                <param>param2=${param2}</param>
                <param>param3=${param3}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <kill name="kill">
            <message>test failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
        </kill>
        <end name="end"/>
    </workflow-app>
    """

    val multipleDecisionCaseWf2 = """
    <workflow-app name="test" xmlns="uri:oozie:workflow:0.2">
        <start to="decision"/>
        <decision name="decision">
            <switch>
                <case to="hive_node">${doHive_other}</case>
                <case to="hive_node2">${doHive2}</case>
                <default to="end"/>
            </switch>
        </decision>
        <action name="hive_node">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script.hql</script>
                <param>dateString=${dateString}</param>
                <param>param1=${param1}</param>
                <param>param2=${param2}</param>
                <param>param3=${param3}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <action name="hive_node2">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script2.hql</script>
                <param>dateString=${dateString}</param>
                <param>param1=${param1}</param>
                <param>param2=${param2}</param>
                <param>param3=${param3}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <kill name="kill">
            <message>test failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
        </kill>
        <end name="end"/>
    </workflow-app>
    """

    val reOrderedDecisionCaseWf = """
    <workflow-app name="test" xmlns="uri:oozie:workflow:0.2">
        <start to="decision"/>
        <decision name="decision">
            <switch>
                <case to="hive_node2">${doHive2}</case>
                <case to="hive_node">${doHive}</case>
                <default to="end"/>
            </switch>
        </decision>
        <action name="hive_node">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script.hql</script>
                <param>dateString=${dateString}</param>
                <param>param1=${param1}</param>
                <param>param2=${param2}</param>
                <param>param3=${param3}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <action name="hive_node2">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script2.hql</script>
                <param>dateString=${dateString}</param>
                <param>param1=${param1}</param>
                <param>param2=${param2}</param>
                <param>param3=${param3}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <kill name="kill">
            <message>test failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
        </kill>
        <end name="end"/>
    </workflow-app>
    """

    val forkWf = """
    <workflow-app name="test" xmlns="uri:oozie:workflow:0.2">
        <start to="fork"/>
        <fork name="fork">
            <path start="second_path"/>
            <path start="first_path"/>
        </fork>
        <action name="first_path">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script.hql</script>
                <param>dateString=${dateString}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <action name="second_path">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script2.hql</script>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <kill name="kill">
            <message>test failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
        </kill>
        <end name="end"/>
    </workflow-app>
    """

    val reOrderedForkWf = """
    <workflow-app name="test" xmlns="uri:oozie:workflow:0.2">
        <start to="fork"/>
        <fork name="fork">
            <path start="path_z1"/>
            <path start="path_2"/>
        </fork>
        <action name="path_z1">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script.hql</script>
                <param>dateString=${dateString}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <action name="path_2">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script2.hql</script>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <kill name="kill">
            <message>test failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
        </kill>
        <end name="end"/>
    </workflow-app>
    """

    val differentForkWf = """
    <workflow-app name="test" xmlns="uri:oozie:workflow:0.2">
        <start to="fork"/>
        <fork name="fork">
            <path start="first_path"/>
            <path start="second_path"/>
        </fork>
        <action name="first_path">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script2.hql</script>
                <param>dateString=${dateString}</param>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <action name="second_path">
            <hive xmlns="uri:oozie:hive-action:0.2">
                <job-tracker>${jobTracker}</job-tracker>
                <name-node>${nameNode}</name-node>
                <job-xml>../hive-site.xml</job-xml>
                <configuration>
                    <property>
                        <name>oozie.hive.defaults</name>
                        <value>../hive-default.xml</value>
                    </property>
                </configuration>
                <script>script.hql</script>
                <file>../oozie-setup.hql</file>
            </hive>
            <ok to="end"/>
            <error to="kill"/>
        </action>
        <kill name="kill">
            <message>test failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
        </kill>
        <end name="end"/>
    </workflow-app>
    """
}
