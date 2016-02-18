/**
  * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
  */

package com.klout.scoozie.runner

import java.util.{Date, Properties}

import com.klout.scoozie.conversion.Conversion
import com.klout.scoozie.dsl._
import com.klout.scoozie.jobs.NoOpJob
import com.klout.scoozie.workflow.WorkflowImpl
import oozie.workflow.{WORKFLOWu45APP, WORKFLOWu45APPOption}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.oozie.client.{OozieClient, WorkflowAction, WorkflowJob}

import scala.collection.JavaConverters._

case class OozieConfig(oozieUrl: String, properties: Map[String, String])

abstract class ScoozieApp[T, ActionOption](
                                            wf: WorkflowApp,
                                            propertiesFile: Option[String] = None,
                                            properties: Option[Map[String, String]] = None,
                                            postprocessing: Option[XmlPostProcessing] = Some(XmlPostProcessing.Default))
  extends App {

  /*
 * Usage is java -cp <...> com.klout.scoozie.ObjectName today
 * -todayString=foo2 -yesterdayString=foo3 ...
 */
  override def main(args: Array[String]) {
    System.setProperty("user.name", "svc_wow")
    val argString = ("" /: args) (_ + _.toString)
    //set up properties / configuration
    val usage = "java -cp <...> com.klout.scoozie.ObjectName -todayString=foo -yesterdayString=foo ..."
    var propertyMap = properties.getOrElse(propertiesFile.map(Helpers.readProperties).get)
    if (args nonEmpty) {
      args foreach { arg =>
        if (arg(0) != '-')
          throw new RuntimeException("error: usage is " + usage)
        propertyMap = Helpers.addProperty(propertyMap, arg.tail)
      }
    }
    val rawAppPath = propertyMap.get("scoozie.wf.application.path").get
    val appPath = {
      if (!rawAppPath.endsWith(".xml")) {
        val suffix = propertyMap.get("pathSuffix") match {
          case Some(toSuffix) => "_" + toSuffix
          case None => ""
        }
        rawAppPath + "scoozie_" + wf.name + suffix + ".xml"
      } else
        throw new RuntimeException("error: you should not overwrite the .xml")
    }
    val oozieUrl = propertyMap.get("scoozie.oozie.url").get
    val config = OozieConfig(oozieUrl, propertyMap)
    //run
    RunWorkflow(wf, appPath, config, postprocessing) match {
      case Left(_) => throw new RuntimeException("error: workflow execution failed")
      case Right(_) => Unit
    }
  }
}

//abstract class CliApp(wfs: List[Workflow], postprocessing: Option[XmlPostProcessing] = Some(XmlPostProcessing.Default)) extends App {
//
//    override def main(args: Array[String]) {
//        var continue = true
//        while (continue) {
//            println("choose a workflow for more information: ")
//            var index = 0
//            wfs map (wf => {
//                println(index + " -> " + wf.name)
//                index += 1
//            })
//            val ln = io.Source.stdin.getLines
//            val choice = ln.next.toInt
//            if (choice < 0 || choice >= wfs.length) {
//                println("invalid choice")
//            } else {
//                println(wfs(choice))
//                val ln = io.Source.stdin.getLines
//                println("generate xml for this workflow? y/n")
//                val print = ln.next
//                print match {
//                    case "y" =>
//                        val xmlString = RunWorkflow.getXMLString(wfs(choice), postprocessing)
//                        println("input filename to write output to")
//                        val outName = ln.next
//                        println("print to screen? y/n")
//                        val p2Screen = ln.next
//                        if (p2Screen == "y")
//                            println(xmlString)
//                        writeToFile(xmlString, outName)
//                    case _ =>
//                }
//            }
//            println("exit? y/n")
//            val exit = ln.next
//            exit match {
//                case "y" => continue = false
//                case _   => continue = true
//            }
//        }
//    }
//
//    def writeToFile(toWrite: String, outfile: String) = {
//        val out = new PrintWriter(new FileWriter(outfile))
//        out.println(toWrite)
//        out.close()
//    }
//}

object Helpers {

  /*
 * Requires: propertyFile is the path to a property file containing properties formatted as follows:
 *   PropertyName1=PropertyValue1
 *   PropertyName2=PropertyValue2
 *   ...
 */
  def readProperties(propertyFile: String): Map[String, String] = {
    var propertyMap: Map[String, String] = Map.empty
    io.Source.fromFile(propertyFile).getLines.foreach { line =>
      if (!line.isEmpty && line(0) != '#') {
        propertyMap = addProperty(propertyMap, line)
      }
    }
    propertyMap
  }

  def addProperty(propertyMap: Map[String, String], propertyString: String): Map[String, String] = {
    val property: Array[String] = propertyString.split("=")
    if (property.length != 2)
      throw new RuntimeException("error: property file not correctly formatted")
    propertyMap + (property(0) -> property(1))
  }

  def retryable[T](body: () => T): T = {
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
}

object Workflows {
  def MaxwellPipeline: WorkflowApp = {
    val featureGen = NoOpJob("FeatureGeneration") dependsOn Start
    val scoreCalc = NoOpJob("ScoreCalculation") dependsOn featureGen
    val momentGen = NoOpJob("MomentGeneration") dependsOn scoreCalc
    val end = End dependsOn momentGen
    WorkflowImpl("maxwell-pipeline-wf", end)
  }

}

case class RetryableOozieClient(client: OozieClient) {

  def run(conf: Properties): String = Helpers.retryable {
    () => client.run(conf)
  }

  def createConfiguration(): Properties = Helpers.retryable {
    () => client.createConfiguration()
  }

  def getJobInfo(jobId: String): WorkflowJob = Helpers.retryable {
    () => client.getJobInfo(jobId)
  }

  def getJobLog(jobId: String): String = Helpers.retryable {
    () => client.getJobLog(jobId)
  }

}

case class XmlPostProcessing(
                              substitutions: Map[String, String])

object XmlPostProcessing {
  val Default = XmlPostProcessing(
    substitutions = Map(
      "&quot;" -> "\"")
  )
}

case class OozieSuccess(jobId: String)

case class OozieError(jobId: String, jobLog: String, consoleUrl: String)

case class AsyncOozieWorkflow(oc: RetryableOozieClient, jobId: String, consoleUrl: String) {
  def jobInfo(): WorkflowJob = oc.getJobInfo(jobId)

  def jobLog(): String = oc.getJobLog(jobId)

  def isRunning(): Boolean = (jobInfo.getStatus() == WorkflowJob.Status.RUNNING)

  def isSuccess(): Boolean = (jobInfo.getStatus() == WorkflowJob.Status.SUCCEEDED)

  def actions(): List[WorkflowAction] = jobInfo().getActions.asScala.toList

  def successOrFail(): Either[OozieError, OozieSuccess] = {
    if (!this.isSuccess()) {
      Left(OozieError(jobId, jobLog(), consoleUrl))
    } else {
      Right(OozieSuccess(jobId))
    }
  }
}

object RunWorkflow {
  val SleepInterval = 5000

  def sequence[T](workflowMap: Map[T, AsyncOozieWorkflow]): Map[T, Either[OozieError, OozieSuccess]] = {
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

  def async[T, ActionOption](workflow: WorkflowApp, appPath: String, config: OozieConfig, postprocessing: Option[XmlPostProcessing]): AsyncOozieWorkflow = {
    prepWorkflow(workflow, appPath, config.properties, postprocessing)
    getOozieWorkflow(appPath, config)
  }

  def apply[T, ActionOption](workflow: WorkflowApp, appPath: String, config: OozieConfig, postprocessing: Option[XmlPostProcessing]): Either[OozieError, OozieSuccess] = {
    prepWorkflow(workflow, appPath, config.properties, postprocessing)
    execWorkflow(appPath, config)
  }

  def prepWorkflow[T, ActionOption](workflow: WorkflowApp, appPathString: String, properties: Map[String, String], postprocessing: Option[XmlPostProcessing]) = {
    val xmlString = getXMLString(workflow, postprocessing)
    val resolvedAppPath = resolveProperty(appPathString, properties)
    val appPath: Path = new Path(resolvedAppPath)
    //write xml file to hdfs
    val conf = new Configuration()
    conf.set("fs.defaultFs", properties.get("nameNode") match {
      case Some(prop) => prop
      case _ => throw new RuntimeException("error: no name node set")
    })
    val fs = FileSystem.get(conf)
    println("About to create path: " + appPath)
    writeFile(fs, appPath, xmlString)
  }

  def writeFile(fs: FileSystem, appPath: Path, data: String) = Helpers.retryable {
    () => {
      if (fs.exists(appPath)) {
        println("It exists, so deleting it first.")
        fs.delete(appPath, false)
      }
      val out: FSDataOutputStream = fs.create(appPath)
      out.write(data.getBytes("UTF-8"))
      out.close
    }
  }

  /*
 * Resolves all ${foo} values in input property
 */
  def resolveProperty(prop: String, properties: Map[String, String]): String = {
    var newProp = prop
    while (newProp.contains("${")) {
      val subVar = newProp.substring(newProp.indexOf("${"), newProp.indexOf("}") + 1)
      val subVarName = subVar.substring(2, subVar.length - 1)
      val subVar_value = properties.get(subVarName) match {
        case Some(value) => value
        case _ => throw new RuntimeException("error: missing property value " + subVarName)
      }
      newProp = newProp.replace(subVar, subVar_value)
    }
    newProp
  }

  /*
 * Executes the workflow, retrying if necessary
 */
  def getOozieWorkflow(appPath: String, config: OozieConfig): AsyncOozieWorkflow = {
    val oc = RetryableOozieClient(new OozieClient(config.oozieUrl))
    // create a workflow job configuration and set the workflow application path
    val conf = oc.createConfiguration()
    //set workflow parameters
    config.properties foreach (pair => conf.setProperty(pair._1, pair._2))
    conf.setProperty(OozieClient.APP_PATH, appPath)
    //submit and start the workflow job
    val jobId: String = oc.run(conf);
    val wfJob: WorkflowJob = oc.getJobInfo(jobId)
    val consoleUrl: String = wfJob.getConsoleUrl
    println(s"Workflow job $jobId submitted and running")
    println("Workflow: " + wfJob.getAppName + " at " + wfJob.getAppPath)
    println("Console URL: " + consoleUrl)
    AsyncOozieWorkflow(oc, jobId, consoleUrl)
  }

  def execWorkflow(appPath: String, config: OozieConfig): Either[OozieError, OozieSuccess] = {

    val async = getOozieWorkflow(appPath, config)

    sequence(Map("blah" -> async)).map(_._2).toList.headOption match {
      case Some(result) => result
      case _ => async.successOrFail()
    }
  }

  def getXMLString(workflow: Workflow[WORKFLOWu45APP, WORKFLOWu45APPOption], postprocessing: Option[XmlPostProcessing] = Some(XmlPostProcessing.Default)): String = {
    val defaultScope = scalaxb.toScope(None -> "uri:oozie:workflow:0.5")
    val wf = Conversion[WORKFLOWu45APP, WORKFLOWu45APPOption](workflow)
    val wfXml = scalaxb.toXML[WORKFLOWu45APP](wf, Some("workflow"), "workflow-app", defaultScope)
    val prettyPrinter = new scala.xml.PrettyPrinter(Int.MaxValue, 4)
    val formattedXml = prettyPrinter.formatNodes(wfXml)
    val processedXml = postprocessing match {
      case Some(proccessingRules) => (formattedXml /: proccessingRules.substitutions) ((str, mapping) => str replace(mapping._1, mapping._2))
      case _ => formattedXml
    }
    processedXml
  }

}