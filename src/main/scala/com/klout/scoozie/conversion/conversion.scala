/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie.conversion

import com.google.common.base._
import com.klout.scoozie.dsl.{ Workflow, Job, DecisionNode, Predicate, Predicates }
import com.klout.scoozie.verification.Verification
import oozie.workflow._

import scalaxb._

case class PartiallyOrderedNode(node: GraphNode,
                                partialOrder: Int)

case object PartiallyOrderedNode {
    def lt(x: PartiallyOrderedNode, y: PartiallyOrderedNode): Boolean = {
        x.partialOrder < y.partialOrder || (x.partialOrder == y.partialOrder && x.node.name < y.node.name)
    }
}

case class GraphNode(var name: String,
                     var workflowOption: WorkflowOption,
                     var before: RefSet[GraphNode],
                     var after: RefSet[GraphNode],
                     var decisionBefore: RefSet[GraphNode] = RefSet(),
                     var decisionAfter: RefSet[GraphNode] = RefSet(),
                     var decisionRoutes: Set[(String, DecisionNode)] = Set.empty,
                     var errorTo: Option[GraphNode] = None) {

    def getName(n: GraphNode) = n.name

    def beforeNames = before map (getName(_))

    def afterNames = after map (getName(_))

    def decisionBeforeNames = decisionBefore map (getName(_))

    def decisionAfterNames = decisionAfter map (getName(_))

    override def toString =
        s"GraphNode: name=[$name], option=[$workflowOption], before=[$beforeNames], after=[$afterNames], decisionBefore=[$decisionBeforeNames], decisionAfter=[$decisionAfterNames], decisionRoute=[$decisionRoutes], errorTo=[$errorTo]"

    override def equals(any: Any): Boolean = {
        any match {
            case node: GraphNode =>
                this.name == node.name &&
                    this.workflowOption == node.workflowOption &&
                    this.beforeNames == node.beforeNames &&
                    this.afterNames == node.afterNames &&
                    this.decisionRoutes == node.decisionRoutes &&
                    this.errorTo == node.errorTo &&
                    this.decisionAfterNames == node.decisionAfterNames &&
                    this.decisionBeforeNames == node.decisionBeforeNames
            case _ => false
        }
    }

    override def hashCode: Int = {
        Objects.hashCode(name, workflowOption, beforeNames, afterNames, decisionBeforeNames, decisionAfterNames, decisionRoutes, errorTo)
    }

    /*
* Checks whether this GraphNode has the desired decisionRoute
*/
    def containsDecisionRoute(predicateRoute: String, decisionNode: DecisionNode): Boolean = {
        decisionRoutes contains (predicateRoute -> decisionNode)
    }

    /*
* Gets route node name for specified predicate route - Only applies for
* Decisions
*/
    private def nameRoutes(predicateRoutes: List[String]): String = {
        this.workflowOption match {
            case WorkflowDecision(predicates, decisionNode) =>
                predicateRoutes map { predicateRoute =>
                    decisionAfter.find(_.containsDecisionRoute(predicateRoute, decisionNode)) match {
                        case Some(routeNode) => routeNode.name
                        case _               => "kill"
                    }
                } mkString "-"
            case _ => throw new RuntimeException("error: getting route from non-decision node")
        }
    }

    def getDecisionRouteName(predicateRoute: String): String = {
        nameRoutes(List(predicateRoute))
    }

    def getDecisionName(predicateRoutes: List[String]): String = {
        nameRoutes("default" :: predicateRoutes)
    }

    lazy val toXmlWorkflowOption: Set[DataRecord[WORKFLOWu45APPOption]] = {
        if (after.size > 1) {
            workflowOption match {
                //after will be of size > 1 for forks
                case WorkflowFork =>
                case _ =>
                    throw new RuntimeException("error: nodes should only be singly linked " + afterNames)
            }
        }
        val okTransition = afterNames.headOption.getOrElse(
            decisionAfterNames.headOption.getOrElse("end"))
        workflowOption match {
            case WorkflowFork =>
                Set(DataRecord(None, Some("fork"), FORK(
                    path = (afterNames.toSeq map FORK_TRANSITION.apply).toList,
                    name = name)))
            case WorkflowJoin =>
                Set(DataRecord(None, Some("join"), JOIN(
                    name = name,
                    to = okTransition)))
            case WorkflowJob(job) =>
                Set(DataRecord(None, Some("action"), ACTION(
                    name = name,
                    actionoption = job.record,
                    ok = ACTION_TRANSITION(okTransition),
                    error = ACTION_TRANSITION(errorTo match {
                        case Some(node) => node.name
                        case _          => "kill"
                    }))))
            case WorkflowDecision(predicates, _) =>
                val defaultName = getDecisionRouteName("default")
                val caseSeq = predicates map (pred => {
                    val route = getDecisionRouteName(pred._1)
                    CASE(Conversion convertPredicate pred._2, route)
                })
                Set(DataRecord(None, Some("decision"), DECISION(
                    name = name,
                    switch = SWITCH(
                        switchsequence1 = SWITCHSequence1(
                            caseValue = caseSeq,
                            default = DEFAULT(defaultName))))))
            case _ => ???
        }
    }
}

object GraphNode {
    def apply(name: String, workflowOption: WorkflowOption): GraphNode =
        GraphNode(name, workflowOption, RefSet(), RefSet())
}

sealed trait WorkflowOption

case class WorkflowJob[A](job: Job[A]) extends WorkflowOption

case class WorkflowDecision(predicates: List[(String, Predicate)], decisionNode: DecisionNode) extends WorkflowOption

case object WorkflowFork extends WorkflowOption

case object WorkflowJoin extends WorkflowOption

case object WorkflowEnd extends WorkflowOption

object Conversion {
    val JobTracker = "${jobTracker}"
    val NameNode = "${nameNode}"

    def apply(workflow: Workflow): WORKFLOWu45APP = {
        val flattenedNodes = Flatten(workflow).values.toSet
        val finalGraph = Verification.verify(flattenedNodes)
        val orderedNodes = order(RefSet(finalGraph.toSeq)).toList sortWith PartiallyOrderedNode.lt map (_.node)
        val workflowOptions = orderedNodes flatMap (_.toXmlWorkflowOption)
        val startTo: String = orderedNodes.headOption match {
            case Some(node) => node.name
            case _          => "end"
        }

        WORKFLOWu45APP(
            name = workflow.name,
            workflowu45appoption = workflowOptions.toList :+ DataRecord(None, Some("kill"), KILL(
                message = workflow.name + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                name = "kill")),
            start = START(startTo),
            end = END("end"))
    }

    def convertPredicate(pred: Predicate): String = {
        pred match {
            case Predicates.AlwaysTrue                => "true"
            case pred @ Predicates.BooleanProperty(_) => pred.formattedProperty
        }
    }

    def isDescendent(child: GraphNode, ancestor: GraphNode): Boolean = {
        if (child == ancestor)
            true
        else if (child.before == Set.empty && child.decisionBefore == Set.empty)
            false
        else (child.before ++ child.decisionBefore).exists(isDescendent(_, ancestor))
    }

    /*
* Requires: from is a descendent of to
*/
    def getMaxDistFromNode(from: GraphNode, to: GraphNode): Int = {
        if (!isDescendent(from, to))
            Int.MaxValue
        else if (from eq to)
            0
        else 1 + ((from.before ++ from.decisionBefore) map ((currNode: GraphNode) => getMaxDistFromNode(currNode, to)) max)
    }

    def order(nodes: RefSet[GraphNode]): RefSet[PartiallyOrderedNode] = {
        // find node with no before nodes
        val startNodes = nodes.filter(n => Flatten.isStartNode(n))
        val startNode = startNodes.headOption
        nodes map ((currNode: GraphNode) => {
            if (startNodes contains currNode)
                PartiallyOrderedNode(currNode, 0)
            else {
                val from = currNode
                val to = startNode.get
                val dist = getMaxDistFromNode(from, to)
                PartiallyOrderedNode(currNode, dist)
            }
        })
    }
}
