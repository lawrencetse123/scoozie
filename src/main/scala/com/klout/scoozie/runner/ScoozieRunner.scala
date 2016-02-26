/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie.runner

import java.io.{ FileWriter, PrintWriter }
import java.util.{ Date, Properties }

import oozie.coordinator.COORDINATORu45APP
import com.klout.scoozie.Scoozie
import com.klout.scoozie.conversion.Conversion
import com.klout.scoozie.dsl._
import oozie.workflow.{ WORKFLOWu45APP, WORKFLOWu45APPOption }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FSDataOutputStream, FileSystem, Path }
import org.apache.oozie.client.{ Job, OozieClient, WorkflowAction, WorkflowJob }

import scala.collection.JavaConverters._

case class OozieConfig(oozieUrl: String, properties: Map[String, String])

sealed trait ScoozieAppType {
    val name: String // = this.getClass.getSimpleName.dropRight(1)
    val oozieConfigKey: String
}

object BundleApp extends ScoozieAppType {
    val name = "bundle"
    val oozieConfigKey = OozieClient.BUNDLE_APP_PATH
}

object CoordinatorApp extends ScoozieAppType {
    val name = "coordinator"
    val oozieConfigKey = OozieClient.COORDINATOR_APP_PATH
}

object WorkflowApp extends ScoozieAppType {
    val name = "workflow"
    val oozieConfigKey = OozieClient.APP_PATH
}

private[scoozie] abstract class ScooizeApp(val appType: ScoozieAppType,
                                           val appName: String,
                                           private val propertiesFile: Option[String] = None,
                                           private val properties: Option[Map[String, String]] = None)
    extends App {

    System.setProperty("user.name", "svc_wow") //hack to set user
    val argString = ("" /: args) (_ + _.toString)
    //set up properties / configuration
    val usage = "java -cp <...> com.your.scoozie.app.ObjectName -todayString=foo -yesterdayString=foo ..."
    var propertyMap = properties.getOrElse(propertiesFile.map(Helpers.readProperties).get)
    if (args nonEmpty) {
        args foreach { arg =>
            if (arg(0) != '-')
                throw new RuntimeException("error: usage is " + usage)
            propertyMap = Helpers.addProperty(propertyMap, arg.tail)
        }
    }
    val oozieUrl = propertyMap.get("scoozie.oozie.url").get
    val config = OozieConfig(oozieUrl, propertyMap)

    val rootAppPath = propertyMap.get(s"scoozie.${appType.oozieConfigKey}").get

    private val SleepInterval = 5000

    def prepWorkflow[T, ActionOption](workflow: WorkflowApp, postprocessing: Option[XmlPostProcessing]) = {
        val xmlString = RunWorkflow.getXMLString(workflow, postprocessing)
        val appPathString = appPath(WorkflowApp)
        //write xml file to hdfs
        RunWorkflow.writeJobXML(appPathString, propertyMap, xmlString)

    }

    def async[T, ActionOption](workflow: WorkflowApp, appPath: String, config: OozieConfig, postprocessing: Option[XmlPostProcessing], appType: ScoozieAppType): AsyncOozieWorkflow = {
        prepWorkflow(workflow, postprocessing)
        getOozieWorkflow(appPath, config, appType)
    }

    def runWorkflow[T, ActionOption](workflow: WorkflowApp, appPath: String, config: OozieConfig, postprocessing: Option[XmlPostProcessing], appType: ScoozieAppType): Either[OozieError, OozieSuccess] = {
        prepWorkflow(workflow, postprocessing)
        execWorkflow(appPath, config, appType)
    }

    def appPath(appType: ScoozieAppType) = {
        if (!rootAppPath.endsWith(".xml")) {
            val suffix = propertyMap.get("pathSuffix") match {
                case Some(toSuffix) => "_" + toSuffix
                case None           => ""
            }
            rootAppPath + s"scoozie_${appType.name}_$appName$suffix.xml"
        } else
            throw new RuntimeException("error: you should not overwrite the .xml")
    }

    /**
     * Executes the workflow, retrying if necessary
     */
    def getOozieWorkflow(appPath: String, config: OozieConfig, appType: ScoozieAppType): AsyncOozieWorkflow = {
        val oc = RetryableOozieClient(new OozieClient(config.oozieUrl))
        // create a workflow job configuration and set the workflow application path
        val conf = oc.createConfiguration()
        //set workflow parameters
        config.properties foreach (pair => conf.setProperty(pair._1, pair._2))
        conf.setProperty(appType.oozieConfigKey, appPath)
        //submit and start the workflow job
        val jobId: String = oc.run(conf)
        val wfJob: WorkflowJob = oc.getJobInfo(jobId)
        val consoleUrl: String = wfJob.getConsoleUrl
        println(s"Workflow job $jobId submitted and running")
        println("Workflow: " + wfJob.getAppName + " at " + wfJob.getAppPath)
        println("Console URL: " + consoleUrl)
        AsyncOozieWorkflow(oc, jobId, consoleUrl)
    }

    def execWorkflow(appPath: String, config: OozieConfig, appType: ScoozieAppType): Either[OozieError, OozieSuccess] = {
        val async = getOozieWorkflow(appPath, config, appType)

        println(config)
        config.properties.foreach(println)

        sequence(Map("blah" -> async)).map(_._2).toList.headOption match {
            case Some(result) => result
            case _            => async.successOrFail()
        }
    }

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

}

/**
 *
 * @param coord
 * @param propertiesFile
 * @param properties
 * @param postprocessing
 */
abstract class ScoozieCoordinatorApp(coord: COORDINATORu45APP,
                                     wf: WorkflowApp,
                                     propertiesFile: Option[String] = None,
                                     properties: Option[Map[String, String]] = None,
                                     postprocessing: Option[XmlPostProcessing] = Some(XmlPostProcessing.Default))
    extends ScooizeApp(CoordinatorApp, coord.name, properties = properties, propertiesFile = propertiesFile) {
    val config = org.ty
    //write workflow
    prepWorkflow(wf, postprocessing)

    val coordXML = Scoozie(coordinator = coord)
    println(coordXML)

    val coordPath = appPath(appType)

    //write coord xml
    RunWorkflow.writeJobXML(coordPath, propertyMap, coordXML)

    //Stop old coords
    RunWorkflow.removeCoordinatorJob(appName, config)

    //Run Coord
    execWorkflow(coordPath, config, appType)
}

/**
 *
 * @param wf
 * @param propertiesFile
 * @param properties
 * @param postprocessing
 * @tparam T
 * @tparam ActionOption
 */
abstract class ScoozieWorkflowApp[T, ActionOption](wf: WorkflowApp,
                                                   propertiesFile: Option[String] = None,
                                                   properties: Option[Map[String, String]] = None,
                                                   postprocessing: Option[XmlPostProcessing] = Some(XmlPostProcessing.Default))
    extends ScooizeApp(WorkflowApp, wf.name, properties = properties, propertiesFile = propertiesFile) {

    runWorkflow(wf, appPath(appType), config, postprocessing, appType) match {
        case Left(_)  => throw new RuntimeException("error: workflow execution failed")
        case Right(_) => Unit
    }
}

abstract class CliApp(wfs: List[WorkflowApp], postprocessing: Option[XmlPostProcessing] = Some(XmlPostProcessing.Default)) extends App {

    override def main(args: Array[String]) {
        var continue = true
        while (continue) {
            println("choose a workflow for more information: ")
            var index = 0
            wfs map (wf => {
                println(index + " -> " + wf.name)
                index += 1
            })
            val ln = io.Source.stdin.getLines
            val choice = ln.next.toInt
            if (choice < 0 || choice >= wfs.length) {
                println("invalid choice")
            } else {
                println(wfs(choice))
                val ln = io.Source.stdin.getLines
                println("generate xml for this workflow? y/n")
                val print = ln.next
                print match {
                    case "y" =>
                        val xmlString = RunWorkflow.getXMLString(wfs(choice), postprocessing)
                        println("input filename to write output to")
                        val outName = ln.next
                        println("print to screen? y/n")
                        val p2Screen = ln.next
                        if (p2Screen == "y")
                            println(xmlString)
                        writeToFile(xmlString, outName)
                    case _ =>
                }
            }
            println("exit? y/n")
            val exit = ln.next
            exit match {
                case "y" => continue = false
                case _   => continue = true
            }
        }
    }

    def writeToFile(toWrite: String, outfile: String) = {
        val out = new PrintWriter(new FileWriter(outfile))
        out.println(toWrite)
        out.close()
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

case class XmlPostProcessing(substitutions: Map[String, String])

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

    def writeJobXML(appPathString: String, properties: Map[String, String], xmlData: String): Unit = {
        val resolvedAppPath = resolveProperty(appPathString, properties)
        val appPath: Path = new Path(resolvedAppPath)
        println("About to create path: " + appPath)
        val conf = new Configuration()
        conf.set("fs.defaultFs", properties.get("nameNode") match {
            case Some(prop) => prop
            case _          => throw new RuntimeException("error: no name node set")
        })
        val fs = FileSystem.get(conf)
        writeFile(fs, appPath, xmlData)
    }

    private def writeFile(fs: FileSystem, appPath: Path, data: String) = Helpers.retryable {
        () =>
            {
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
                case _           => throw new RuntimeException("error: missing property value " + subVarName)
            }
            newProp = newProp.replace(subVar, subVar_value)
        }
        newProp
    }

    def getXMLString(workflow: WorkflowApp, postprocessing: Option[XmlPostProcessing] = Some(XmlPostProcessing.Default)): String = {
        val defaultScope = scalaxb.toScope(None -> "uri:oozie:workflow:0.5")
        val wf = Conversion[WORKFLOWu45APP, WORKFLOWu45APPOption](workflow)
        val wfXml = scalaxb.toXML[WORKFLOWu45APP](wf, Some("workflow"), "workflow-app", defaultScope)
        val prettyPrinter = new scala.xml.PrettyPrinter(Int.MaxValue, 4)
        val formattedXml = prettyPrinter.formatNodes(wfXml)
        val processedXml = postprocessing match {
            case Some(proccessingRules) => (formattedXml /: proccessingRules.substitutions) ((str, mapping) => str replace (mapping._1, mapping._2))
            case _                      => formattedXml
        }
        processedXml
    }

    def removeCoordinatorJob(appName: String, config: OozieConfig): Unit = {
        val oc = RetryableOozieClient(new OozieClient(config.oozieUrl))

        import scala.collection.JavaConversions._
        val coordJobsToRemove = oc.client.getCoordJobsInfo(s"NAME=$appName", 1, 100).filter{
            cj => cj.getAppName == appName && cj.getStatus == Job.Status.RUNNING
        }.map(_.getId).toSeq

        coordJobsToRemove.foreach(oc.client.kill)
    }

}