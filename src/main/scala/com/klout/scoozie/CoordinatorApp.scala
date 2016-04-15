package com.klout.scoozie

import com.klout.scoozie.dsl.Coordinator
import com.klout.scoozie.runner.ScoozieApp
import com.klout.scoozie.utils.ExecutionUtils
import com.klout.scoozie.writer.{ FileSystemUtils, XmlPostProcessing }

import scalaxb.CanWriteXML

abstract class CoordinatorApp[C: CanWriteXML, W: CanWriteXML](coordinator: Coordinator[C, W],
                                                              oozieUrl: String,
                                                              appPath: String,
                                                              fileSystemUtils: FileSystemUtils,
                                                              properties: Option[Map[String, String]] = None,
                                                              postProcessing: XmlPostProcessing = XmlPostProcessing.Default) extends ScoozieApp(properties) {

  import com.klout.scoozie.writer.implicits._
  coordinator.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)
  ExecutionUtils.removeCoordinatorJob(coordinator.name, oozieUrl)
  ExecutionUtils.run(oozieUrl, coordinator.getJobProperties(appPath, jobProperties))
}