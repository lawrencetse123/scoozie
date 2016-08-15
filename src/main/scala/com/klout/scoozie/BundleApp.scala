package com.klout.scoozie

import com.klout.scoozie.dsl.Bundle
import com.klout.scoozie.runner.BundleAppAbs
import com.klout.scoozie.utils.ExecutionUtils
import com.klout.scoozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scala.concurrent.Future
import scalaxb.CanWriteXML

class BundleApp[B: CanWriteXML,C: CanWriteXML, W: CanWriteXML](override val bundle: Bundle[B, C, W],
                                                               oozieUrl: String,
                                                               override val appPath: String,
                                                               override val fileSystemUtils: FileSystemUtils,
                                                               override val properties: Option[Map[String, String]] = None,
                                                               override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
    extends BundleAppAbs[B, C, W] {
  override val oozieClient: OozieClient = new OozieClient(oozieUrl)

  import com.klout.scoozie.writer.implicits._

  override lazy val executionResult: Future[Job] =
    ExecutionUtils.run[OozieClient, Job, JobStatus](oozieClient, bundle.getJobProperties(appPath, jobProperties))
}