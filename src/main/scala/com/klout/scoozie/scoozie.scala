/**
  * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
  */

package com.klout.scoozie

import java.io.{File, PrintWriter}

import com.klout.scoozie.conversion.Conversion
import com.klout.scoozie.dsl.Workflow
import oozie.bundle.BUNDLEu45APP
import oozie.coordinator.COORDINATORu45APP

import scala.util.Try
import scala.xml.NodeSeq
import scalaxb.CanWriteXML
import scalaxb.`package`._

object ScoozieConfig {
  val targetFolderName = "oozie"
  val workflowFolderName = "workflows"
  val coordinatorFolderName = "coordinators"
  val bundleFolderName = "bundles"
  val propertyFileName = "job.properties"
  val rootFolderParameterName = "rootFolder"
}

object Scoozie {
  def apply[T: CanWriteXML, K](workflow: Workflow[T, K], postProcessing: Option[XmlPostProcessing]): String = {
    val wf = Conversion(workflow)
    generateXml(wf, workflow.scope, workflow.namespace, workflow.elementLabel, postProcessing)
  }

  def apply(coordinator: COORDINATORu45APP, postProcessing: Option[XmlPostProcessing]): String =
    generateXml(coordinator, "uri:oozie:coordinator:0.4", "coordinator", "coordinator-app", postProcessing)

  def apply(bundle: BUNDLEu45APP, postProcessing: Option[XmlPostProcessing]): String = {
    generateXml(bundle, "uri:oozie:bundle:0.2", "bundle", "bundle-app", postProcessing)
  }

  def apply[T: CanWriteXML, K](workflow: Workflow[T, K]): String = {
    apply(workflow, Some(XmlPostProcessing.Default))
  }

  def apply(coordinator: COORDINATORu45APP): String = {
    apply(coordinator, Some(XmlPostProcessing.Default))
  }

  def apply(bundle: BUNDLEu45APP): String = {
    apply(bundle, Some(XmlPostProcessing.Default))
  }

  def write[T: CanWriteXML, K](rootFolder: String,
                               workflow: Workflow[T, K],
                               properties: Map[String, String],
                               fileSystemUtils: FileSystemUtils): Try[Unit] = {

    import utils.PropertyImplicits._

    val pathBuilder = new PathBuilder(rootFolder)
    import pathBuilder._
    import ScoozieConfig._

    val workflowFilename = withXmlExtension(workflow.name)

    val updatedProperties = properties ++ Map(
      ScoozieConfig.rootFolderParameterName -> rootFolder,
      "oozie.wf.application.path" ->
        ("${" + ScoozieConfig.rootFolderParameterName + "}" +
          s"/$targetFolderName/$workflowFolderName/$workflowFilename")
    )

    for {
      _ <- fileSystemUtils.makeDirectory(getTargetFolderPath)
      _ <- fileSystemUtils.makeDirectory(getWorkflowFolderPath)
      _ <- fileSystemUtils.writeTextFile(getPropertiesFilePath, updatedProperties.toProperties.toWritableString)
      _ <- fileSystemUtils.writeTextFile(getWorkflowFilePath(workflowFilename), Scoozie(workflow))
    } yield ()
  }

  def write[T: CanWriteXML, K](rootFolder: String,
                               workflow: Workflow[T, K],
                               properties: Map[String, String]): Try[Unit] = {

    write(rootFolder, workflow, properties, LocalFileSystemUtils)
  }

  def write[T: CanWriteXML, K](rootFolder: String,
                               coordinator: COORDINATORu45APP,
                               workflow: Workflow[T, K],
                               properties: Map[String, String],
                               fileSystemUtils: FileSystemUtils): Try[Unit] = {

    import utils.PropertyImplicits._

    val pathBuilder = new PathBuilder(rootFolder)
    import pathBuilder._
    import ScoozieConfig._

    val coordinatorFileName = withXmlExtension(coordinator.name)

    val updatedProperties = properties ++ Map(
      ScoozieConfig.rootFolderParameterName -> rootFolder,
      "oozie.coord.application.path" -> ("${" + ScoozieConfig.rootFolderParameterName + "}" +
        s"/$targetFolderName/$coordinatorFolderName/$coordinatorFileName")
    )

    for {
      _ <- fileSystemUtils.makeDirectory(getTargetFolderPath)
      _ <- fileSystemUtils.makeDirectory(getWorkflowFolderPath)
      _ <- fileSystemUtils.makeDirectory(getCoordinatorFolderPath)
      _ <- fileSystemUtils.writeTextFile(getPropertiesFilePath, updatedProperties.toProperties.toWritableString)
      _ <- fileSystemUtils.writeTextFile(getCoordinatorFilePath(coordinatorFileName), Scoozie(coordinator))
      _ <- fileSystemUtils.writeTextFile(getWorkflowFilePath(withXmlExtension(workflow.name)), Scoozie(workflow))
    } yield ()
  }

  def write[T: CanWriteXML, K](rootFolder: String,
                               coordinator: COORDINATORu45APP,
                               workflow: Workflow[T, K],
                               properties: Map[String, String]): Try[Unit] = {

    write(rootFolder, coordinator, workflow, properties, LocalFileSystemUtils)
  }

  def write[T: CanWriteXML, K](rootFolder: String,
                               bundle: BUNDLEu45APP,
                               coordinators: Seq[COORDINATORu45APP],
                               workflows: Seq[Workflow[T, K]],
                               properties: Map[String, String],
                               fileSystemUtils: FileSystemUtils): Try[Unit] = {

    import utils.PropertyImplicits._

    val pathBuilder = new PathBuilder(rootFolder)
    import pathBuilder._
    import ScoozieConfig._

    val bundleFileName = withXmlExtension(bundle.name)

    val updatedProperties = properties ++ Map(
      ScoozieConfig.rootFolderParameterName -> rootFolder,
      "oozie.bundle.application.path" -> ("${" + ScoozieConfig.rootFolderParameterName + "}" +
        s"/$targetFolderName/$bundleFolderName/$bundleFileName")
    )

    import com.klout.scoozie.utils.SeqImplicits._

    def writeWorkflows() = {
      for {
        _ <- fileSystemUtils.makeDirectory(getWorkflowFolderPath)
        _ <- sequenceTry(workflows.map(workflow =>
          fileSystemUtils
            .writeTextFile(
              getWorkflowFilePath(withXmlExtension(workflow.name)),
              Scoozie(workflow))))
      } yield ()
    }

    def writeCoordinators() = {
      for {
        _ <- fileSystemUtils.makeDirectory(getCoordinatorFolderPath)
        _ <- sequenceTry(coordinators.map(coordinator =>
          fileSystemUtils
            .writeTextFile(
              getCoordinatorFilePath(withXmlExtension(coordinator.name)),
              Scoozie(coordinator))))
      } yield ()
    }

    for {
      _ <- fileSystemUtils.makeDirectory(getTargetFolderPath)
      _ <- fileSystemUtils.makeDirectory(getBundleFolderPath)
      _ <- fileSystemUtils.writeTextFile(getPropertiesFilePath, updatedProperties.toProperties.toWritableString)
      _ <- fileSystemUtils.writeTextFile(getBundleFilePath(bundleFileName), Scoozie(bundle))
      _ <- writeCoordinators() if coordinators.nonEmpty
      _ <- writeWorkflows() if workflows.nonEmpty
    } yield ()
  }

  def write[T: CanWriteXML, K](rootFolder: String,
                               bundle: BUNDLEu45APP,
                               coordinators: Seq[COORDINATORu45APP],
                               workflows: Seq[Workflow[T, K]],
                               properties: Map[String, String]): Try[Unit] = {

    write(rootFolder, bundle, coordinators, workflows, properties, LocalFileSystemUtils)
  }

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
        (formattedXml /: proccessingRules.substitutions) ((str, mapping) => str replace(mapping._1, mapping._2))
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

trait FileSystemUtils {
  def writeTextFile(path: String, text: String): Try[Unit]

  def makeDirectory(path: String): Try[Unit]
}

object LocalFileSystemUtils extends FileSystemUtils {
  override def writeTextFile(path: String, text: String): Try[Unit] = {
    import com.klout.scoozie.utils.TryImplicits._

    val pw = new PrintWriter(new File(path))
    Try(pw.write(text)).doFinally(pw.close())
  }

  override def makeDirectory(path: String): Try[Unit] = {
    Try(new File(path).mkdir())
  }
}

class PathBuilder(rootFolderPath: String) {

  import ScoozieConfig._

  def getTargetFolderPath: String = s"$rootFolderPath/$targetFolderName"

  def getWorkflowFolderPath: String = s"$getTargetFolderPath/$workflowFolderName"

  def getCoordinatorFolderPath: String = s"$getTargetFolderPath/$coordinatorFolderName"

  def getBundleFolderPath: String = s"$getTargetFolderPath/$bundleFolderName"

  def getPropertiesFilePath: String = s"$getTargetFolderPath/$propertyFileName"

  def getWorkflowFilePath(workflowFileName: String): String = s"$getWorkflowFolderPath/$workflowFileName"

  def getCoordinatorFilePath(coordinatorFileName: String): String = s"$getCoordinatorFolderPath/$coordinatorFileName"

  def getBundleFilePath(bundleFileName: String): String = s"$getBundleFolderPath/$bundleFileName"

  def withXmlExtension(name: String): String = s"$name.xml"
}

object `package` {
  def ???[T] = sys.error("not implemented yet!")
}