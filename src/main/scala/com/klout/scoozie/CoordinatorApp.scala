package com.klout.scoozie

import com.klout.scoozie.dsl.Coordinator
import com.klout.scoozie.runner.CoordinatorAppAbs
import com.klout.scoozie.utils.ExecutionUtils
import com.klout.scoozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scalaxb.CanWriteXML

class CoordinatorApp[C: CanWriteXML, W: CanWriteXML](override val coordinator: Coordinator[C, W],
                                                     oozieUrl: String,
                                                     override val appPath: String,
                                                     override val fileSystemUtils: FileSystemUtils,
                                                     override val properties: Option[Map[String, String]] = None,
                                                     override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
    extends CoordinatorAppAbs[C, W] {
  override val oozieClient: OozieClient = new OozieClient(oozieUrl)

  import com.klout.scoozie.writer.implicits._

  override lazy val executionResult =
    ExecutionUtils.run[OozieClient, Job, JobStatus](oozieClient, coordinator.getJobProperties(appPath, jobProperties))
}