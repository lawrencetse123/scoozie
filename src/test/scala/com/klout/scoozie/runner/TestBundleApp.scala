package com.klout.scoozie.runner

import com.klout.scoozie.dsl.Bundle
import com.klout.scoozie.utils.ExecutionUtils._
import com.klout.scoozie.writer.{XmlPostProcessing, FileSystemUtils}
import org.apache.oozie.client.OozieClient

import scala.concurrent.Future
import scalaxb.CanWriteXML

class TestBundleApp[B: CanWriteXML, C: CanWriteXML, W: CanWriteXML](override val bundle: Bundle[B, C, W],
                                                                    override val oozieClient: OozieClient,
                                                                    override val appPath: String,
                                                                    override val fileSystemUtils: FileSystemUtils,
                                                                    override val properties: Option[Map[String, String]] = None,
                                                                    override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
  extends BundleAppAbs[B, C, W] {

  import com.klout.scoozie.writer.implicits._

  val executionResult: Future[Job] =
    run[OozieClient, Job, JobStatus](oozieClient, bundle.getJobProperties(appPath, jobProperties))
}
