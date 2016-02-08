/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie

import com.klout.scoozie.conversion.Conversion
import com.klout.scoozie.dsl.Workflow
import oozie.workflow.WORKFLOWu45APP

import scalaxb.`package`._

object Scoozie {
    def apply(workflow: Workflow, postprocessing: Option[XmlPostProcessing] = Some(XmlPostProcessing.Default)): String = {
        val defaultScope = scalaxb.toScope(None -> "uri:oozie:workflow:0.5")
        val wf = Conversion(workflow)
        val wfXml = toXML[WORKFLOWu45APP](wf, Some("workflow"), "workflow-app", defaultScope)
        val prettyPrinter = new scala.xml.PrettyPrinter(Int.MaxValue, 4)
        val formattedXml = prettyPrinter.formatNodes(wfXml)
        val processedXml = postprocessing match {
            case Some(proccessingRules) => (formattedXml /: proccessingRules.substitutions) ((str, mapping) => str replace (mapping._1, mapping._2))
            case _                      => formattedXml
        }
        processedXml
    }
}

case class XmlPostProcessing(substitutions: Map[String, String])

object XmlPostProcessing {
    val Default = XmlPostProcessing(
        substitutions = Map(
            "&quot;" -> "\"")
    )
}

object `package` {
    def ???[T] = sys.error("not implemented yet!")
}