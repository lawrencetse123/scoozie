package com.klout.scoozie.writer

import com.klout.scoozie.ScoozieConfig._
import com.klout.scoozie.dsl.{ Bundle, Coordinator, Workflow }
import com.klout.scoozie.utils.WriterImplicitConversions._
import com.klout.scoozie.utils.WriterUtils._

import scala.util.Try
import scalaxb.CanWriteXML

package object implicits {
  trait CanWrite {
    def toXml(xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): String

    def write(path: String,
              fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
              xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit]

    def writeJob(path: String,
                 properties: Option[Map[String, String]] = None,
                 fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
                 xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit]

    def getJobProperties(path: String,
                         properties: Option[Map[String, String]] = None): Map[String, String]
  }

  implicit class WorkflowWriter[W: CanWriteXML](underlying: Workflow[W]) extends CanWrite {
    override def write(path: String,
                       fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
                       xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      fileSystemUtils.writeTextFile(path, underlying.toXmlString(xmlPostProcessing))
    }

    override def writeJob(path: String,
                          properties: Option[Map[String, String]] = None,
                          fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
                          xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      val pathBuilder: PathBuilder = new PathBuilder(path)

      import pathBuilder._

      val workflowFilename = withXmlExtension(underlying.name)

      import com.klout.scoozie.utils.PropertyImplicits._
      val propertiesString = getJobProperties(path, properties).toProperties.toWritableString

      for {
        _ <- fileSystemUtils.makeDirectory(getTargetFolderPath)
        _ <- fileSystemUtils.makeDirectory(getWorkflowFolderPath)
        _ <- fileSystemUtils.writeTextFile(getPropertiesFilePath, propertiesString)
        _ <- fileSystemUtils.writeTextFile(getWorkflowFilePath(workflowFilename), underlying.toXmlString(xmlPostProcessing))
      } yield ()
    }

    override def toXml(xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default) = underlying.toXmlString(xmlPostProcessing)

    override def getJobProperties(path: String,
                                  properties: Option[Map[String, String]] = None): Map[String, String] = {

      buildProperties(
        path,
        "oozie.wf.application.path",
        s"/$workflowFolderName/${withXmlExtension(underlying.name)}",
        properties)
    }
  }

  implicit class CoordinatorWriter[C: CanWriteXML, W: CanWriteXML](underlying: Coordinator[C, W]) extends CanWrite {
    override def write(path: String,
                       fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
                       xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      assertPathIsDefined(underlying.workflowPath, "Workflow", underlying.workflow.name)

      fileSystemUtils.writeTextFile(path, underlying.toXmlString(xmlPostProcessing))
    }

    override def writeJob(path: String,
                          properties: Option[Map[String, String]] = None,
                          fileSystemUtils: FileSystemUtils,
                          xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      assertPathIsEmpty(underlying.workflowPath, "Workflow", underlying.workflow.name)

      val pathBuilder: PathBuilder = new PathBuilder(path)

      import pathBuilder._

      val coordinatorFilename = withXmlExtension(underlying.name)

      val workflowName = underlying.workflow.name
      val workflowPath = getWorkflowFilePath(withXmlExtension(workflowName))

      import com.klout.scoozie.utils.PropertyImplicits._
      val propertiesString = getJobProperties(path, properties).toProperties.toWritableString

      for {
        _ <- fileSystemUtils.makeDirectory(getTargetFolderPath)
        _ <- fileSystemUtils.makeDirectory(getWorkflowFolderPath)
        _ <- fileSystemUtils.makeDirectory(getCoordinatorFolderPath)
        _ <- fileSystemUtils.writeTextFile(getPropertiesFilePath, propertiesString)
        _ <- fileSystemUtils.writeTextFile(
          getCoordinatorFilePath(coordinatorFilename),
          underlying.toXmlString(xmlPostProcessing))
        _ <- fileSystemUtils.writeTextFile(workflowPath, underlying.workflow.toXmlString(xmlPostProcessing))
      } yield ()
    }

    override def toXml(xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default) =
      underlying.toXmlString(xmlPostProcessing)

    override def getJobProperties(path: String,
                                  properties: Option[Map[String, String]] = None): Map[String, String] = {

      val coordinatorFilename = withXmlExtension(underlying.name)
      val workflowName = underlying.workflow.name

      buildProperties(
        rootPath = path,
        applicationProperty = "oozie.coord.application.path",
        applicationPath = s"/$coordinatorFolderName/$coordinatorFilename",
        properties = Some(properties.getOrElse(Map[String, String]()) + createPathProperty(workflowName, workflowFolderName)))
    }
  }

  implicit class BundleWriter[B: CanWriteXML, C: CanWriteXML, W: CanWriteXML, A](underlying: Bundle[B, C, W]) extends CanWrite {
    override def write(path: String,
                       fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
                       xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      underlying.coordinators.foreach(descriptor => {
        assertPathIsDefined(descriptor.path, "Coordinator", descriptor.coordinator.name)
      })

      fileSystemUtils.writeTextFile(path, underlying.toXmlString(xmlPostProcessing))
    }

    override def writeJob(path: String,
                          properties: Option[Map[String, String]] = None,
                          fileSystemUtils: FileSystemUtils,
                          xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      underlying.coordinators.foreach(descriptor => {
        assertPathIsEmpty(descriptor.path, "Coordinator", descriptor.coordinator.name)
        assertPathIsEmpty(descriptor.coordinator.workflowPath, "Workflow", descriptor.coordinator.workflow.name)
      })

      val pathBuilder: PathBuilder = new PathBuilder(path)

      import com.klout.scoozie.utils.SeqImplicits._
      import pathBuilder._

      val bundleFileName = withXmlExtension(underlying.name)

      import com.klout.scoozie.utils.PropertyImplicits._
      val propertiesString = getJobProperties(path, properties).toProperties.toWritableString

      def writeWorkflows() = {
        for {
          _ <- fileSystemUtils.makeDirectory(getWorkflowFolderPath)
          _ <- sequenceTry(underlying.coordinators.map(descriptor =>
            fileSystemUtils
              .writeTextFile(
                getWorkflowFilePath(withXmlExtension(descriptor.coordinator.workflow.name)),
                descriptor.coordinator.workflow.toXml(xmlPostProcessing))))
        } yield ()
      }

      def writeCoordinators() = {
        for {
          _ <- fileSystemUtils.makeDirectory(getCoordinatorFolderPath)
          _ <- sequenceTry(underlying.coordinators.map(descriptor =>
            fileSystemUtils
              .writeTextFile(
                getCoordinatorFilePath(withXmlExtension(descriptor.coordinator.name)),
                descriptor.coordinator.toXml(xmlPostProcessing))))
        } yield ()
      }

      for {
        _ <- fileSystemUtils.makeDirectory(getTargetFolderPath)
        _ <- fileSystemUtils.makeDirectory(getBundleFolderPath)
        _ <- fileSystemUtils.writeTextFile(getPropertiesFilePath, propertiesString)
        _ <- fileSystemUtils.writeTextFile(getBundleFilePath(bundleFileName), underlying.toXml(xmlPostProcessing))
        _ <- writeCoordinators()
        _ <- writeWorkflows()
      } yield ()
    }

    override def toXml(xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default) = underlying.toXmlString(xmlPostProcessing)

    override def getJobProperties(path: String,
                                  properties: Option[Map[String, String]] = None): Map[String, String] = {
      val bundleFileName = withXmlExtension(underlying.name)

      val pathProperties = underlying.coordinators.flatMap(descriptor => {
        List(
          createPathProperty(descriptor.coordinator.workflow.name, workflowFolderName),
          createPathProperty(descriptor.coordinator.name, coordinatorFolderName))
      }).toSet

      buildProperties(
        rootPath = path,
        applicationProperty = "oozie.bundle.application.path",
        applicationPath = s"/$bundleFolderName/$bundleFileName",
        properties = Some(properties.getOrElse(Map[String, String]()) ++ pathProperties))
    }
  }
}