/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie.jobs

import com.klout.scoozie.dsl.Job
import oozie.workflow_0_5._
import oozie.workflow_0_5.hive.{ ACTION => HIVEACTION }
import oozie._

import scalaxb.DataRecord

case class GenericJob[A](jobName: String, record: DataRecord[A]) extends Job[A]

case class MapReduceJob(name: String,
                        nameNode: String = JobUtils.nameNode,
                        jobTracker: String = JobUtils.jobTracker,
                        prepare: List[FsTask] = List.empty,
                        configuration: ConfigurationList = Nil) extends Job[MAPu45REDUCE] {
    override val jobName = s"mr_$name"
    override val record = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
        jobu45tracker = Some(jobTracker),
        nameu45node = Some(nameNode),
        prepare = JobUtils.getPrepare(prepare),
        configuration = JobUtils.getConfiguration(configuration)))
}

case class HiveJob(fileName: String,
                   configuration: ConfigurationList = Nil,
                   parameters: List[String] = List.empty,
                   prepare: List[FsTask] = List.empty,
                   jobXml: Option[Seq[String]] = None,
                   otherFiles: Option[Seq[String]] = None) extends Job[HIVEACTION] {
    val dotIndex = fileName.indexOf(".")
    val cleanName = {
        if (dotIndex > 0)
            fileName.substring(0, dotIndex)
        else
            fileName
    }
    override val jobName = s"hive_$cleanName"
    override val record = DataRecord(None, Some("hive"), HIVEACTION(
        jobu45tracker = Some(JobUtils.jobTracker),
        nameu45node = Some(JobUtils.nameNode),
        jobu45xml = jobXml match {
            case Some(xml) => xml
            case _         => Seq[String]()
        },
        configuration = JobUtils.getConfiguration(configuration),
        actionoption = DataRecord(fileName), //this is probably the incorrect parameter
        param = parameters.toSeq,
        prepare = JobUtils.getPrepare(prepare),
        file = otherFiles.getOrElse(Nil),
        xmlns = "uri:oozie:hive-action:0.5"
    ))
}

// Node: There is a limitation with the way scalaxb creates the FS Task
// case classes from workflow.xsd: It treats the different task types as
// separate sequences so ordering among the types is not possible.
// Need to address later.
case class FsJob(name: String, tasks: List[FsTask]) extends Job[FS] {
    override val jobName = s"fs_$name"
    override val record = DataRecord(None, Some("fs"), FS( //        delete = tasks flatMap {
    //            case Rm(path) => Some(DELETE(path))
    //            case _        => Nonel
    //        },
    //        mkdir = tasks flatMap {
    //            case MkDir(path) => Some(MKDIR(path))
    //            case _           => None
    //        },
    //        move = tasks flatMap {
    //            case Mv(from, to) => Some(MOVE(from, to))
    //            case _            => None
    //        },
    //        chmod = tasks flatMap {
    //            case ChMod(path, permissions, dirFiles) => Some(CHMOD(path, permissions, Some(dirFiles)))
    //            case _                                  => None
    //        })
    ))
}

sealed trait FsTask

case class MkDir(path: String) extends FsTask

case class Mv(from: String, to: String) extends FsTask

case class Rm(path: String) extends FsTask

case class Touchz(path: String) extends FsTask

case class ChMod(path: String, permissions: String, dirFiles: String) extends FsTask

case class JavaJob(mainClass: String,
                   prepare: List[FsTask] = List.empty,
                   configuration: ConfigurationList = Nil,
                   jvmOps: Option[String] = None,
                   args: List[String] = Nil) extends Job[JAVA] {
    val domain = mainClass.substring(mainClass.lastIndexOf(".") + 1)
    override val jobName = s"java_$domain"
    override val record = DataRecord(None, Some("java"), JAVA(
        jobu45tracker = Some(JobUtils.jobTracker),
        nameu45node = Some(JobUtils.nameNode),
        mainu45class = mainClass,
        prepare = JobUtils.getPrepare(prepare),
        configuration = JobUtils.getConfiguration(configuration),
        javaoption = Seq(DataRecord(jvmOps.getOrElse(""))),
        arg = args))
}

case class NoOpJob(name: String) extends Job[String] {
    override val jobName = name
    override val record = DataRecord("")
}

object `package` {
    type Name = String
    type Value = String
    type ConfigurationList = List[(Name, Value)]
    type ParameterList = List[(Name, Option[Value])]
}

object JobUtils {
    val jobTracker: String = "${jobTracker}"
    val nameNode: String = "${nameNode}"

    def getPrepare(prep: List[FsTask]) = {
        val deletes: Seq[DELETE] = prep flatMap {
            case Rm(path) => Some(DELETE(path))
            case _        => None
        }
        val mkdirs: Seq[MKDIR] = prep flatMap {
            case MkDir(path) => Some(MKDIR(path))
            case _           => None
        }
        if (!(deletes.isEmpty && mkdirs.isEmpty))
            Some(PREPARE(deletes, mkdirs))
        else
            None
    }

    def getConfiguration(config: ConfigurationList) = {
        if (config.nonEmpty)
            Some(oozie.workflow_0_5.CONFIGURATION(config map (tuple => Property2(tuple._1, tuple._2))))
        else
            None
    }
}