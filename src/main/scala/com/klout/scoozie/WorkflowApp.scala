package com.klout.scoozie

import com.klout.scoozie.dsl.Workflow
import com.klout.scoozie.runner.WorkflowAppAbs
import com.klout.scoozie.utils.ExecutionUtils
import com.klout.scoozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scala.concurrent.Future
import scalaxb.CanWriteXML

class WorkflowApp[W: CanWriteXML](override val workflow: Workflow[W],
                                  oozieUrl: String,
                                  override val appPath: String,
                                  override val fileSystemUtils: FileSystemUtils,
                                  override val properties: Option[Map[String, String]] = None,
                                  override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
    extends WorkflowAppAbs[W] {
  override val oozieClient: OozieClient = new OozieClient(oozieUrl)

  import com.klout.scoozie.writer.implicits._

  override val executionResult: Future[Job] =
    ExecutionUtils.run[OozieClient, Job, JobStatus](oozieClient, workflow.getJobProperties(appPath, jobProperties))
}