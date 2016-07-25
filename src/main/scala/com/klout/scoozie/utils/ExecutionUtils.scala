package com.klout.scoozie.utils

import _root_.retry._
import org.apache.oozie.client._

import scala.concurrent.Future
import scala.concurrent.duration._

object ExecutionUtils {
  def toProperty(propertyString: String): (String, String) = {
    val property: Array[String] = propertyString.split("=")
    if (property.length != 2) throw new RuntimeException("error: property file not correctly formatted")
    else property(0) -> property(1)
  }

  def removeCoordinatorJob(appName: String, oozieClient: OozieClient): Unit = {
    import scala.collection.JavaConversions._
    val coordJobsToRemove = oozieClient.getCoordJobsInfo(s"NAME=$appName", 1, 100).filter{
      cj => cj.getAppName == appName && cj.getStatus == Job.Status.RUNNING
    }.map(_.getId).toSeq

    coordJobsToRemove.foreach(oozieClient.kill)
  }

  def run[T <: OozieClient, K, J](oozieClient: T, properties: Map[String, String])(implicit ev: OozieClientLike[T, K, J]): Future[K] ={
    println("Starting Execution")

    val conf = oozieClient.createConfiguration()
    properties.foreach { case (key, value) => conf.setProperty(key, value) }

    // Rerun success conditions
    implicit val startJobSuccess = Success[String](!_.isEmpty)
    implicit val getJobStatusSuccess = Success[J](x => !(x == ev.prep || x == ev.running))

    // Rerun policies
    val retry = Backoff(5, 500.millis)
    val retryForever = Pause.forever(1.second)

    import scala.concurrent.ExecutionContext.Implicits.global

    def startJob: Future[String] = retry(() => Future({
        val id: String = oozieClient.run(conf)
        println(s"Started job: $id")
        id
    }))

    def retryJobStatus(id: String): Future[J] = retryForever(() =>
      Future({
        val status = ev.getJobStatus(oozieClient, id)
        println(s"JOB: $id $status")
        status
      })
    )

    for {
      jobId <- startJob
      status <- retryJobStatus(jobId)
      job <- Future(ev.getJobInfo(oozieClient, jobId))
    } yield {
      if (status != ev.succeeded) throw new Exception(s"The job was not successful. Completed with status: $status")
      else job
    }
  }
}

trait OozieClientLike[Client, Job, JobStatus] {
  val running: JobStatus
  val prep: JobStatus
  val succeeded: JobStatus
  def getJobInfo(oozieClient: Client, jobId: String): Job
  def getJobStatus(oozieClient: Client, jobId: String): JobStatus
}

object OozieClientLike {
  implicit object OozieClientLikeCoord extends OozieClientLike[OozieClient, Job, Job.Status] {
    val running: Job.Status = Job.Status.RUNNING
    val prep: Job.Status = Job.Status.PREP
    val succeeded: Job.Status = Job.Status.SUCCEEDED
    def getJobInfo(oozieClient: OozieClient, jobId: String): Job = oozieClient.getCoordJobInfo(jobId)
    def getJobStatus(oozieClient: OozieClient, jobId: String): Job.Status = getJobInfo(oozieClient, jobId).getStatus
  }

  implicit object OozieClientLikeExecutable extends OozieClientLike[OozieClient, WorkflowJob, WorkflowJob.Status] {
    val running: WorkflowJob.Status = WorkflowJob.Status.RUNNING
    val prep: WorkflowJob.Status = WorkflowJob.Status.PREP
    val succeeded: WorkflowJob.Status = WorkflowJob.Status.SUCCEEDED
    def getJobInfo(oozieClient: OozieClient, id: String): WorkflowJob = oozieClient.getJobInfo(id)
    def getJobStatus(oozieClient: OozieClient, id: String): WorkflowJob.Status = getJobInfo(oozieClient, id).getStatus
  }
}