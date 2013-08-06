/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie
package verification

import scalaxb._
import workflow._
import scala.xml._

object XMLVerification {

    def verify(referenceString: String, targetString: String): Boolean = {
        val referenceWf = getWorkflow(referenceString)
        val targetWf = getWorkflow(targetString)
        val referenceNodes: Map[String, WORKFLOWu45APPOption] = getNameNodeMap(referenceWf.workflowu45appoption)
        val targetNodes: Map[String, WORKFLOWu45APPOption] = getNameNodeMap(targetWf.workflowu45appoption)

        //will recur on start nodes of wf
        val firstRefNode = getNodeByName(referenceNodes, referenceWf.start.to)
        val firstTargetNode = getNodeByName(targetNodes, targetWf.start.to)

        areFunctionallySame(firstRefNode, firstTargetNode, referenceNodes, targetNodes)
    }

    def getNameNodeMap(wf: Seq[scalaxb.DataRecord[WORKFLOWu45APPOption]]): Map[String, WORKFLOWu45APPOption] = {
        wf.map(node => (getNodeName(node), node.value)) toMap
    }

    def getNodeName(node: DataRecord[WORKFLOWu45APPOption]): String = {
        node.value match {
            case DECISION(switch, name) => name
            case FORK(path, name) => name
            case JOIN(name, to) => name
            case KILL(message, name) => name
            case ACTION(actionoption, ok, error, any, name) => name
            case _ => throw new RuntimeException("error: unexpected node type")
        }
    }

    def areFunctionallySame(ref: WORKFLOWu45APPOption, target: WORKFLOWu45APPOption, refNodes: Map[String, WORKFLOWu45APPOption], targetNodes: Map[String, WORKFLOWu45APPOption]): Boolean = {
        val refNode = makeNamesBlank(ref)
        val targetNode = makeNamesBlank(target)

        (targetNode, refNode) match {

            case (ACTION(targetActionoption, targetOk, targetError, targetAny, targetName), ACTION(refActionoption, refOk, refError, refAny, refName)) =>
                val sameConfiguration = refActionoption == targetActionoption
                val sameNextNodes = nextNodesAreSame(refOk, targetOk, refError, targetError, refNodes, targetNodes)
                val areSame = sameConfiguration && sameNextNodes
                if (!sameConfiguration) {
                    println("node configurations do not match: \n" + refActionoption + "\n" + targetActionoption)
                }
                areSame

            case (ref: KILL, target: KILL) => true

            case (DECISION(targetSwitch, targetName), DECISION(refSwitch, refName)) =>
                val sameDecisionCases = decisionCasesAreSame(refSwitch.switchsequence1, targetSwitch.switchsequence1, refNodes, targetNodes)
                if (!sameDecisionCases)
                    println("decision cases are not the same: \n" + refSwitch + "\n" + targetSwitch)
                sameDecisionCases

            case (target, ref) =>
                if (ref.getClass != target.getClass) {
                    println("nodes not of same type: \n" + refNode + "\n" + targetNode)
                    false
                } else
                    ref == target
        }
    }

    def sortHiveParams(hive: ACTIONType): ACTIONType = {
        val sortedParams = hive.param.sorted
        hive.copy(param = sortedParams)
    }

    def fromXMLString[A: XMLFormat](xmlString: String): A = {
        fromXML[A](xml.XML.loadString(xmlString))
    }

    def getWorkflow(xmlString: String): WORKFLOWu45APP = {
        val workflow = fromXMLString[WORKFLOWu45APP](xmlString)
        convertHive(workflow)
    }

    def makeNamesBlank(node: WORKFLOWu45APPOption): WORKFLOWu45APPOption = {
        node match {
            case DECISION(switch, name) => DECISION(switch, "")
            case FORK(path, name) => FORK(path, "")
            case JOIN(name, to) => JOIN("", to)
            case KILL(message, name) => KILL(message, "")
            case ACTION(actionoption, ok, error, any, name) => ACTION(actionoption, ok, error, any, "")
            case _ => node
        }
    }

    def nextNodesAreSame(refOk: ACTION_TRANSITION, targetOk: ACTION_TRANSITION, refError: ACTION_TRANSITION, targetError: ACTION_TRANSITION, refNodes: Map[String, WORKFLOWu45APPOption], targetNodes: Map[String, WORKFLOWu45APPOption]): Boolean = {
        def oneGoesToEnd(refTransition: ACTION_TRANSITION, targetTransition: ACTION_TRANSITION) =
            refTransition.to == "end" || targetTransition.to == "end"
        def bothGoToEnd(refTransition: ACTION_TRANSITION, targetTransition: ACTION_TRANSITION) =
            refTransition.to == "end" && targetTransition.to == "end"

        val okToMatch = {
            if (oneGoesToEnd(refOk, targetOk))
                bothGoToEnd(refOk, targetOk)
            else
                areFunctionallySame(getNodeByName(refNodes, refOk.to), getNodeByName(targetNodes, targetOk.to), refNodes, targetNodes)
        }
        val errorToMatch = {
            if (oneGoesToEnd(refError, targetError))
                bothGoToEnd(refError, targetError)
            else
                areFunctionallySame(getNodeByName(refNodes, refError.to), getNodeByName(targetNodes, targetError.to), refNodes, targetNodes)
        }
        okToMatch && errorToMatch
    }

    def decisionCasesAreSame(refSwitch: SWITCHSequence1, targetSwitch: SWITCHSequence1, refNodes: Map[String, WORKFLOWu45APPOption], targetNodes: Map[String, WORKFLOWu45APPOption]): Boolean = {
        val refCases = refSwitch.caseValue
        val targetCases = targetSwitch.caseValue
        val optionsSame: Boolean = {
            if (refCases.length != targetCases.length) {
                println("error: decisions don't have same number of paths")
                false
            } else {
                val decisionCasePairs = refCases.zip(targetCases)
                decisionCasePairs.filterNot {
                    case (refCase, targetCase) =>
                        (refCase.to == "end" && targetCase.to == "end") || areFunctionallySame(getNodeByName(refNodes, refCase.to), getNodeByName(targetNodes, targetCase.to), refNodes, targetNodes)
                } isEmpty
            }
        }
        val defaultsSame: Boolean = (refSwitch.default.to == "end" && targetSwitch.default.to == "end") || areFunctionallySame(getNodeByName(refNodes, refSwitch.default.to), getNodeByName(targetNodes, targetSwitch.default.to), refNodes, targetNodes)
        optionsSame && defaultsSame
    }

    /*
     * Takes a workflow class, searches for un-processed Hive xml,
     * and converts it into a parameterized object
     */
    def convertHive(wf: WORKFLOWu45APP): WORKFLOWu45APP = {
        val options = wf.workflowu45appoption.map({ wfOption =>
            wfOption.value match {
                case ACTION(actionOption, ok, error, any, name) =>
                    //if we find an un-parsed Hive action, format it to a case class
                    val action: DataRecord[Any] = actionOption.value match {
                        case elem @ Elem(prefix, "hive", attributes, scope, children @ _*) =>
                            val deDupedScope = NamespaceBinding(null, "uri:oozie:hive-action:0.2", scala.xml.TopScope)
                            val copiedChildren: Seq[Node] = children.toSeq.flatMap(formatHiveNode(_))
                            val newElem = Elem(prefix, "hive", attributes, deDupedScope, copiedChildren: _*)
                            val hiveAction = fromXMLString[ACTIONType](newElem.toString)
                            val processedHiveAction = sortHiveParams(hiveAction)
                            DataRecord(processedHiveAction)
                        case _ =>
                            actionOption
                    }
                    DataRecord(ACTION(action, ok, error, any, name))
                case _ => wfOption
            }
        })
        wf.copy(workflowu45appoption = options)
    }

    /*
     * Format Hive xml elements to get around all the gross name-spacing issues
     */
    def formatHiveNode(node: Node): Node = {
        val hiveNameSpacedNode = Elem(node.prefix, node.label, node.attributes, NamespaceBinding(null, "uri:oozie:hive-action:0.2", scala.xml.TopScope), node.child.toSeq.flatMap(formatHiveNode(_)): _*)
        val workflowNameSpacedNode = Elem(node.prefix, node.label, node.attributes, NamespaceBinding(null, "uri:oozie:workflow:0.2", scala.xml.TopScope), node.child.toSeq.flatMap(formatHiveNode(_)): _*)
        val hiveNameSpacedTags = List("job-tracker", "name-node", "prepare", "job-xml", "configuration", "script", "param", "file")
        node match {
            case Elem(prefix, label, attributes, scope, children @ _*) =>
                if (hiveNameSpacedTags contains label)
                    hiveNameSpacedNode
                else
                    workflowNameSpacedNode
            case _ =>
                node
        }
    }

    def getNodeByName(nodes: Map[String, WORKFLOWu45APPOption], nodeName: String): WORKFLOWu45APPOption = {
        nodes.get(nodeName) match {
            case Some(node) => node
            case _          => throw new RuntimeException(s"error: node $nodeName not in workflow")
        }
    }
}

object Verify extends App {
    val refPath = readLine("input reference xml path: ")
    val targetPath = readLine("input target scoozie generated xml path: ")
    val refXml = scala.io.Source.fromFile(refPath).mkString
    val targetXml = scala.io.Source.fromFile(targetPath).mkString
    val areSame = XMLVerification.verify(refXml, targetXml)
    if (areSame)
        println("workflows are functionally equal")
    else
        println("error: workflows are not functionally equal")
}