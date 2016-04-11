package com.klout.scoozie.utils

import com.klout.scoozie.ScoozieConfig
import com.klout.scoozie.writer.XmlPostProcessing

import scala.xml.NodeSeq
import scalaxb.CanWriteXML
import scalaxb.`package`._

object WriterUtils {
    def buildPropertiesString(rootPath: String,
                              applicationProperty: String,
                              applicationPath: String,
                              properties: Option[Map[String, String]]): String = {
        import com.klout.scoozie.utils.PropertyImplicits._

        (properties.getOrElse(Map()) ++ Map(
            ScoozieConfig.rootFolderParameterName -> rootPath,
            applicationProperty -> addRootSubstitutionToPath(applicationPath)
        )).toProperties.toWritableString
    }

    def withXmlExtension(name: String): String = s"$name.xml"

    def addRootSubstitutionToPath(path: String) = "${" + ScoozieConfig.rootFolderParameterName + "}" + path

    def buildPathPropertyName(name: String) = s"$name.path"

    def generateXml[A: CanWriteXML](xmlObject: A,
                                    scope: String,
                                    namespace: String,
                                    elementLabel: String,
                                    postProcessing: Option[XmlPostProcessing]): String = {

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

    def assertPathIsEmpty(path: Option[String], applicationType: String, applicationName: String) = assert(
        assertion = path.isEmpty,
        message = s"The path: ${path.get} was provided for the following $applicationType: $applicationName. This must be None to write a job")

    def assertPathIsDefined(path: Option[String], applicationType: String, applicationName: String) = assert(
        assertion = path.isDefined,
        message = s"A path was not defined for the following $applicationType: $applicationName")

    def createPathProperty(name: String, folderName: String) = {
        val fileName = withXmlExtension(name)
        val substitutedPath = addRootSubstitutionToPath(s"/$folderName/$fileName")

        buildPathPropertyName(name) -> substitutedPath
    }
}
