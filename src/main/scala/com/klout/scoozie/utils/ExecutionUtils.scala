package com.klout.scoozie.utils

import java.util.{Date, Properties}

import org.apache.oozie.client.{Job, OozieClient, WorkflowAction, WorkflowJob}

object ExecutionUtils {
  def toProperty(propertyString: String): (String, String) = {
    val property: Array[String] = propertyString.split("=")
    if (property.length != 2) throw new RuntimeException("error: property file not correctly formatted")
    else property(0) -> property(1)
  }

  def removeCoordinatorJob(appName: String, oozieUrl: String): Unit = {
    val oc = RetryableOozieClient(new OozieClient(oozieUrl))

    import scala.collection.JavaConversions._
    val coordJobsToRemove = oc.client.getCoordJobsInfo(s"NAME=$appName", 1, 100).filter{
      cj => cj.getAppName == appName && cj.getStatus == Job.Status.RUNNING
    }.map(_.getId).toSeq

    coordJobsToRemove.foreach(oc.client.kill)
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // The shit beyond this point needs to be completely rewritten. /////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def run(oozieUrl: String, properties: Map[String, String]): Either[OozieError, OozieSuccess] = {
      val oc = RetryableOozieClient(new OozieClient(oozieUrl))

      // create a workflow job configuration and set the workflow application path
      val conf = oc.createConfiguration()

      //set workflow parameters
      properties.foreach{ case (key, value) => conf.setProperty(key, value) }

      //submit and start the workflow job
      val jobId: String = oc.run(conf)
      val wfJob: WorkflowJob = oc.getJobInfo(jobId)
      val consoleUrl: String = wfJob.getConsoleUrl

      println(s"Oozie application $jobId submitted and running")
      println("Application: " + wfJob.getAppName + " at " + wfJob.getAppPath)
      println("Console URL: " + consoleUrl)

      val async = AsyncOozieWorkflow(oc, jobId, consoleUrl)

      println(oozieUrl, properties)
      properties.foreach(println)

      sequence(Map("blah" -> async)).map(_._2).toList.headOption match {
        case Some(result) => result
        case _            => async.successOrFail()
      }
  }

  private val SleepInterval = 5000

  private def sequence[T](workflowMap: Map[T, AsyncOozieWorkflow]): Map[T, Either[OozieError, OozieSuccess]] = {
    val workflows = workflowMap map (_._2)

    while (workflows exists (_.isRunning)) {
      println("Workflow job running ...")
      workflows flatMap (_.actions) filter (_.getStatus == WorkflowAction.Status.RUNNING) foreach (action => {
        val now = new Date
        println(now + " " + action)
      })
      Thread.sleep(SleepInterval)
    }
    // print the final status to the workflow job
    println("Workflow jobs completed ...")
    workflows foreach (a => println(a.jobInfo()))
    workflowMap mapValues (_.successOrFail())
  }

  private def retryable[T](body: () => T): T = {
    val backoff: Double = 1.5
    def retryable0(body: () => T, remaining: Int, retrySleep: Double): T = {
      try {
        body()
      } catch {
        case t: Throwable =>
          println("ERROR : Unexpected exception ")
          t.printStackTrace
          if (remaining > 0) {
            println("Retries left: " + remaining + ". Sleeping for " + retrySleep)
            Thread.sleep(retrySleep.toLong)
            retryable0(body, remaining - 1, retrySleep * backoff)
          } else
            throw new RuntimeException("error: Allowed number of retries exceeded. Exiting with failure.")
      }
    }
    retryable0(body, 5, 2000)
  }

  case class RetryableOozieClient(client: OozieClient) {
    def run(conf: Properties): String = retryable {
      () => client.run(conf)
    }

    def createConfiguration(): Properties = retryable {
      () => client.createConfiguration()
    }

    def getJobInfo(jobId: String): WorkflowJob = retryable {
      () => client.getJobInfo(jobId)
    }

    def getJobLog(jobId: String): String = retryable {
      () => client.getJobLog(jobId)
    }
  }

  case class OozieSuccess(jobId: String)

  case class OozieError(jobId: String, jobLog: String, consoleUrl: String)

  case class AsyncOozieWorkflow(oc: RetryableOozieClient, jobId: String, consoleUrl: String) {
    def jobInfo(): WorkflowJob = oc.getJobInfo(jobId)

    def jobLog(): String = oc.getJobLog(jobId)

    def isRunning(): Boolean = (jobInfo.getStatus() == WorkflowJob.Status.RUNNING)

    def isSuccess(): Boolean = (jobInfo.getStatus() == WorkflowJob.Status.SUCCEEDED)

    import scala.collection.JavaConverters._
    def actions(): List[WorkflowAction] = jobInfo().getActions.asScala.toList

    def successOrFail(): Either[OozieError, OozieSuccess] = {
      if (!this.isSuccess()) {
        Left(OozieError(jobId, jobLog(), consoleUrl))
      } else {
        Right(OozieSuccess(jobId))
      }
    }
  }
}
