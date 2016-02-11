/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie

import com.klout.scoozie.conversion.Conversion
import com.klout.scoozie.dsl.Workflow
import oozie.bundle.BUNDLEu45APP
import oozie.coordinator.COORDINATORu45APP

import scala.xml.NodeSeq
import scalaxb.CanWriteXML
import scalaxb.`package`._

object Scoozie {
    def apply[T, K](workflow: Workflow[T, K], postProcessing: Option[XmlPostProcessing])(implicit format: CanWriteXML[T]): String = {
        val wf = Conversion(workflow)
        generateXml(wf, workflow.scope, workflow.namespace, workflow.elementLabel, postProcessing)
    }

    def apply(coordinator: COORDINATORu45APP, postProcessing: Option[XmlPostProcessing]): String =
        generateXml(coordinator, "uri:oozie:coordinator:0.4", "coordinator", "coordinator-app", postProcessing)

    def apply(bundle: BUNDLEu45APP, postProcessing: Option[XmlPostProcessing]): String = {
        generateXml(bundle, "uri:oozie:bundle:0.2", "bundle", "bundle-app", postProcessing)
    }

    def apply[T, K](workflow: Workflow[T, K])(implicit format: CanWriteXML[T]): String = {
        apply(workflow, Some(XmlPostProcessing.Default))
    }

    def apply(coordinator: COORDINATORu45APP): String = {
        apply(coordinator, Some(XmlPostProcessing.Default))
    }

    def apply(bundle: BUNDLEu45APP): String = {
        apply(bundle, Some(XmlPostProcessing.Default))
    }

    def generateXml[A](xmlObject: A,
                       scope: String,
                       namespace: String,
                       elementLabel: String,
                       postProcessing: Option[XmlPostProcessing])(implicit format: CanWriteXML[A]): String = {

        val defaultScope = scalaxb.toScope(None -> scope)
        val xml = toXML[A](xmlObject, Some(namespace), elementLabel, defaultScope)
        postProcess(xml, postProcessing)
    }

    def postProcess(input: NodeSeq, postProcessing: Option[XmlPostProcessing]): String = {
        val prettyPrinter = new scala.xml.PrettyPrinter(Int.MaxValue, 4)
        val formattedXml = prettyPrinter.formatNodes(input)
        val processedXml = postProcessing match {
            case Some(proccessingRules) =>
                (formattedXml /: proccessingRules.substitutions) ((str, mapping) => str replace (mapping._1, mapping._2))
            case _ => formattedXml
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