package com.klout.scoozie.runner

import com.klout.scoozie.dsl.Bundle

import scala.concurrent.Future
import scalaxb.CanWriteXML

abstract class BundleAppAbs[B: CanWriteXML,C: CanWriteXML, W: CanWriteXML] extends ScoozieApp {
  val bundle: Bundle[B, C, W]

  type Job = org.apache.oozie.client.Job
  type JobStatus = org.apache.oozie.client.Job.Status

  import com.klout.scoozie.writer.implicits._
  lazy val writeResult = bundle.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)
  logWriteResult()

  val executionResult: Future[Job]
}
