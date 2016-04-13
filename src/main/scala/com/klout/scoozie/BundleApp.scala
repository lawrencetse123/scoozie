package com.klout.scoozie

import com.klout.scoozie.dsl.Bundle
import com.klout.scoozie.runner.ScoozieApp
import com.klout.scoozie.utils.ExecutionUtils
import com.klout.scoozie.writer.{FileSystemUtils, XmlPostProcessing}

import scalaxb.CanWriteXML

abstract class BundleApp[B: CanWriteXML, C: CanWriteXML, W: CanWriteXML](bundle: Bundle[B, C, W],
                                                                         oozieUrl: String,
                                                                         appPath: String,
                                                                         fileSystemUtils: FileSystemUtils,
                                                                         properties: Option[Map[String, String]] = None,
                                                                         postProcessing: XmlPostProcessing = XmlPostProcessing.Default) extends ScoozieApp(properties) {

  import com.klout.scoozie.writer.implicits._
  bundle.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)
  // Need to add a method to remove previously running bundle here
  ExecutionUtils.run(oozieUrl, bundle.getJobProperties(appPath, jobProperties))
}