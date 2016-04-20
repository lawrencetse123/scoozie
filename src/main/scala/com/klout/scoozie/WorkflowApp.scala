package com.klout.scoozie

import com.klout.scoozie.dsl.Workflow
import com.klout.scoozie.runner.ScoozieApp
import com.klout.scoozie.utils.ExecutionUtils
import com.klout.scoozie.writer.{FileSystemUtils, XmlPostProcessing}

import scalaxb.CanWriteXML

abstract class WorkflowApp[W: CanWriteXML](workflow: Workflow[W],
                                           oozieUrl: String,
                                           appPath: String,
                                           fileSystemUtils: FileSystemUtils,
                                           properties: Option[Map[String, String]] = None,
                                           postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
    extends ScoozieApp(properties) {

  import com.klout.scoozie.writer.implicits._

  val writeResult = workflow.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)
  logWriteResult()
  val executionResult = ExecutionUtils.run(oozieUrl, workflow.getJobProperties(appPath, jobProperties))
}
