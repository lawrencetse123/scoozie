/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie
package conversion

import jobs._
import dsl._
import workflow._
import scalaxb._
import com.google.common.base._
import verification._

case class PartiallyOrderedNode(
    node: GraphNode,
    partialOrder: Int)

case object PartiallyOrderedNode {
    def lt(x: PartiallyOrderedNode, y: PartiallyOrderedNode): Boolean = {
        x.partialOrder < y.partialOrder || (x.partialOrder == y.partialOrder && x.node.name < y.node.name)
    }
}

case class GraphNode(
    var name: String,
    var workflowOption: WorkflowOption,
    var before: RefSet[GraphNode],
    var after: RefSet[GraphNode],
    var decisionBefore: RefSet[GraphNode] = RefSet(),
    var decisionAfter: RefSet[GraphNode] = RefSet(),
    var decisionRoute: Option[String] = None,
    var sugarDecisionRoute: Option[String] = None,
    var errorTo: Option[GraphNode] = None) {

    def getName(n: GraphNode) = n.name

    def beforeNames = before map (getName(_))
    def afterNames = after map (getName(_))
    def decisionBeforeNames = decisionBefore map (getName(_))
    def decisionAfterNames = decisionAfter map (getName(_))

    override def toString =
        s"GraphNode: name=[$name], option=[$workflowOption], before=[$beforeNames], after=[$afterNames], decisionBefore=[$decisionBeforeNames], decisionAfter=[$decisionAfterNames], decisionRoute=[$decisionRoute], sugarDecision=[$sugarDecisionRoute], errorTo=[$errorTo]"

    override def equals(any: Any): Boolean = {
        any match {
            case node: GraphNode =>
                this.name == node.name &&
                    this.workflowOption == node.workflowOption &&
                    this.beforeNames == node.beforeNames &&
                    this.afterNames == node.afterNames &&
                    this.decisionRoute == node.decisionRoute &&
                    this.sugarDecisionRoute == node.sugarDecisionRoute &&
                    this.errorTo == node.errorTo
            case _ => false
        }
    }

    override def hashCode: Int = {
        Objects.hashCode(name, workflowOption, beforeNames, afterNames, decisionRoute, sugarDecisionRoute, errorTo)
    }

    /*
     * Checks whether this GraphNode has the desired decisionRoute
     */
    def containsDecisionRoute(predicateRoute: String): Boolean = {
        decisionRoute match {
            case Some(decisionRoute) =>
                decisionRoute == predicateRoute
            case _ => false
        }
    }

    /*
     * Gets route node name for specified predicate route - Only applies for
     * Decisions
     */
    def getDecisionRouteName(predicateRoute: String): String = {
        this.workflowOption match {
            case WorkflowDecision(predicates) =>
                decisionAfter.find(_.containsDecisionRoute(predicateRoute)) match {
                    case Some(routeNode) =>
                        routeNode.name
                    case _ => "kill"
                }
            case _ => throw new RuntimeException("error: getting route from non-decision node")
        }
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
                    actionoption = Conversion convertJob job,
                    ok = ACTION_TRANSITION(okTransition),
                    error = ACTION_TRANSITION(errorTo match {
                        case Some(node) => node.name
                        case _          => "kill"
                    }))))
            case WorkflowDecision(predicates) =>
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

case class WorkflowJob(job: Job) extends WorkflowOption

case class WorkflowDecision(predicates: List[(String, dsl.Predicate)]) extends WorkflowOption

case object WorkflowFork extends WorkflowOption

case object WorkflowJoin extends WorkflowOption

case object WorkflowEnd extends WorkflowOption

object Conversion {
    val JobTracker = "${jobTracker}"
    val NameNode = "${nameNode}"

    def apply(workflow: Workflow): WORKFLOWu45APP = {
        val flattenedNodes = Flatten(workflow).values.toSet
        val finalGraph = Verification.verify(flattenedNodes)
        val orderedNodes = order(RefSet(finalGraph.toSeq)).toList sortWith (PartiallyOrderedNode.lt) map (_.node)
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

    def convertJob(job: Job): DataRecord[Any] = job match {
        case MapReduceJob(name, prep, config) =>
            DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                jobu45tracker = JobTracker,
                nameu45node = NameNode,
                prepare = getPrepare(prep),
                configuration = getConfiguration(config)))

        case HiveJob(fileName, config, params, prep, jobXml, otherFiles) =>
            DataRecord(None, Some("hive"), ACTIONType(
                jobu45tracker = JobTracker,
                nameu45node = NameNode,
                jobu45xml = jobXml match {
                    case Some(xml) => xml
                    case _         => Seq[String]()
                },
                configuration = getConfiguration(config),
                script = fileName,
                param = params.toSeq,
                prepare = getPrepare(prep),
                file = otherFiles.getOrElse(Nil),
                xmlns = "uri:oozie:hive-action:0.2"))

        case JavaJob(mainClass, prep, config, jvmOps, args) =>
            DataRecord(None, Some("java"), JAVA(
                jobu45tracker = JobTracker,
                nameu45node = NameNode,
                mainu45class = mainClass,
                prepare = getPrepare(prep),
                configuration = getConfiguration(config),
                javau45opts = jvmOps,
                arg = args))

        //limitation: tasks must be of the same type
        case FsJob(name, tasks) =>
            DataRecord(None, Some("fs"), FS(
                delete = tasks flatMap {
                    case Rm(path) => Some(DELETE(path))
                    case _        => None
                },
                mkdir = tasks flatMap {
                    case MkDir(path) => Some(MKDIR(path))
                    case _           => None
                },
                move = tasks flatMap {
                    case Mv(from, to) => Some(MOVE(from, to))
                    case _            => None
                },
                chmod = tasks flatMap {
                    case ChMod(path, permissions, dirFiles) => Some(CHMOD(path, permissions, Some(dirFiles)))
                    case _                                  => None
                }))
        case _ => ???
    }

    def getPrepare(prep: List[FsTask]) = {
        val deletes: Seq[DELETE] = prep flatMap {
            case Rm(path) => Some(DELETE(path))
            case _        => None
        }
        val mkdirs: Seq[MKDIR] = prep flatMap {
            case MkDir(path) => Some(MKDIR(path))
            case _           => None
        }
        if (!(deletes.isEmpty && mkdirs.isEmpty))
            Some(PREPARE(deletes, mkdirs))
        else
            None
    }
    def getConfiguration(config: ArgList) = {
        if (!config.isEmpty)
            Some(CONFIGURATION(config map (tuple => Property(tuple._1, tuple._2)): _*))
        else
            None
    }

    def convertPredicate(pred: dsl.Predicate): String = {
        pred match {
            case dsl.Predicates.AlwaysTrue                => "true"
            case pred @ dsl.Predicates.BooleanProperty(_) => pred.formattedProperty
        }
    }

    def isDescendent(child: GraphNode, ancestor: GraphNode): Boolean = {
        if (child == ancestor)
            true
        else if (child.before == Set.empty && child.decisionBefore == Set.empty)
            false
        else (child.before ++ child.decisionBefore).exists (isDescendent(_, ancestor))
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
        val startNodes = nodes.filter (n => Flatten.isStartNode(n))
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
