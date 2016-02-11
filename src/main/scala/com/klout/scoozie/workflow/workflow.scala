package com.klout.scoozie.workflow

import com.klout.scoozie.dsl.{ Workflow, Node, ActionBuilder, Job }
import oozie.workflow._

import scalaxb.DataRecord

class ActionBuilderImpl extends ActionBuilder[WORKFLOWu45APPOption] {
    def buildAction(name: String,
                    actionOption: Job[WORKFLOWu45APPOption],
                    okTo: String,
                    errorTo: String): DataRecord[WORKFLOWu45APPOption] =
        DataRecord(None, Some("action"), ACTION(
            name = name,
            actionoption = actionOption.record,
            ok = ACTION_TRANSITION(okTo),
            error = ACTION_TRANSITION(errorTo)))

    def buildJoin(name: String, okTo: String): DataRecord[WORKFLOWu45APPOption] =
        DataRecord(None, Some("join"), JOIN(name = name, to = okTo))

    def buildFork(name: String, afterNames: List[String]): DataRecord[WORKFLOWu45APPOption] =
        DataRecord(None, Some("fork"), FORK(
            path = afterNames.map(FORK_TRANSITION),
            name = name))

    def buildDecision(name: String,
                      defaultName: String,
                      cases: List[(Predicate, Route)]): DataRecord[WORKFLOWu45APPOption] =
        DataRecord(None, Some("decision"), DECISION(
            name = name,
            switch = SWITCH(
                switchsequence1 = SWITCHSequence1(
                    caseValue = cases.map { case (predicate, route) => CASE(predicate, route) },
                    default = DEFAULT(defaultName)))))

    def buildKill(workflowName: String): DataRecord[WORKFLOWu45APPOption] =
        DataRecord(None, Some("kill"), KILL(
            message = workflowName + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
            name = "kill"))
}

case class WorkflowImpl(name: String,
                        end: Node,
                        parameters: Option[PARAMETERS] = None,
                        global: Option[GLOBAL] = None,
                        credentials: Option[CREDENTIALS] = None,
                        any: Option[oozie.workflow.sla.SLAu45INFO] = None)
    extends Workflow[WORKFLOWu45APP, WORKFLOWu45APPOption] {

    override val scope = "uri:oozie:workflow:0.5"
    override val namespace = "workflow"
    override val elementLabel = "workflow-app"

    override val actionBuilder = new ActionBuilderImpl
    override def buildWorkflow(start: String, end: String, action: Seq[DataRecord[WORKFLOWu45APPOption]]) = {
        WORKFLOWu45APP(name = name,
            start = START(start),
            end = END(end),
            parameters = parameters,
            global = global,
            credentials = credentials,
            workflowu45appoption = action,
            any = any.map(DataRecord(None, Some("sla"), _))
        )
    }
}

