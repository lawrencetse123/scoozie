package com.klout.scoozie.runner

import com.klout.scoozie.dsl._
import com.klout.scoozie.jobs._

/**
 * Created by nathan on 18/02/2016.
 */
object Workflows {
    def MaxwellPipeline: WorkflowApp = {
        val featureGen = NoOpJob("FeatureGeneration") dependsOn Start
        val scoreCalc = NoOpJob("ScoreCalculation") dependsOn featureGen
        val momentGen = NoOpJob("MomentGeneration") dependsOn scoreCalc
        val end = End dependsOn momentGen
        Workflow("maxwell-pipeline-wf", end)
    }

    def generateCoordinator = {
        import oozie.coordinator_0_5._
        //    Scoozie(
        COORDINATORu45APP(
            action = ACTION(WORKFLOW(
                appu45path = "/projects/wow/share/oozie-new/scoozie_workflow_maxwell-pipeline-wf.xml",
                configuration = Some(CONFIGURATION(Seq(
                    Property2(name = "currentDate", value = "${coord:formatTime(coord:nominalTime(), 'yyyyMMdd')}")
                ))))),
            frequency = "${coord:days(1)}",
            start = "2016-02-18T22:00Z",
            end = "2999-01-01T22:00Z",
            timezone = "Australia/Sydney",
            name = "wow-etl-coordinator"
        )
        //    )
    }

}
