package com.klout.scoozie

import com.klout.scoozie.dsl.Workflow
import com.klout.scoozie.runner.WorkflowAppAbs
import com.klout.scoozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scalaxb.CanWriteXML

class WorkflowApp[W: CanWriteXML](override val workflow: Workflow[W],
                                  oozieUrl: String,
                                  override val appPath: String,
                                  override val fileSystemUtils: FileSystemUtils,
                                  override val properties: Option[Map[String, String]] = None,
                                  override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
    extends WorkflowAppAbs[W] {
  override val oozieClient: OozieClient = new OozieClient(oozieUrl)
}