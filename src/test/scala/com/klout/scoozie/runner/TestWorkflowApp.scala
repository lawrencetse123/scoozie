package com.klout.scoozie.runner

import com.klout.scoozie.dsl.Workflow
import com.klout.scoozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scalaxb.CanWriteXML

class TestWorkflowApp[W: CanWriteXML](override val workflow: Workflow[W],
                                      override val oozieClient: OozieClient,
                                      override val appPath: String,
                                      override val fileSystemUtils: FileSystemUtils,
                                      override val properties: Option[Map[String, String]] = None,
                                      override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
  extends WorkflowAppAbs[W] {}
