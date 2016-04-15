package com.klout.scoozie.jobs

import com.klout.scoozie.dsl.{ ConfigBuilder, ConfigurationList, Job }
import oozie.shell_0_3._

import scalaxb.DataRecord

object ShellJob {
  def apply(jobName: String,
            exec: String,
            jobTracker: Option[String] = None,
            nameNode: Option[String] = None,
            prepare: Option[PREPARE] = None,
            jobXml: Seq[String] = Nil,
            configuration: ConfigurationList = Nil,
            arguments: Seq[String] = Nil,
            environmentVariables: Seq[String] = Nil,
            file: Seq[String] = Nil,
            archive: Seq[String] = Nil,
            captureOutput: Boolean = false) = v0_3(
    jobName,
    exec,
    jobTracker,
    nameNode,
    prepare,
    jobXml,
    configuration,
    arguments,
    environmentVariables,
    file,
    archive,
    captureOutput
  )

  def v0_3(jobName: String,
           exec: String,
           jobTracker: Option[String] = None,
           nameNode: Option[String] = None,
           prepare: Option[PREPARE] = None,
           jobXml: Seq[String] = Nil,
           configuration: ConfigurationList = Nil,
           arguments: Seq[String] = Nil,
           environmentVariables: Seq[String] = Nil,
           file: Seq[String] = Nil,
           archive: Seq[String] = Nil,
           captureOutput: Boolean = false) = {

    val configBuilderImpl = new ConfigBuilder[CONFIGURATION, Property] {
      override def build(property: Seq[Property]): CONFIGURATION = CONFIGURATION(property)

      override def buildProperty(name: String, value: String, description: Option[String]): Property = Property(name, value, description)
    }

    val _jobName = jobName

    new Job[ACTION] {
      override val jobName = _jobName

      override val record: DataRecord[ACTION] = DataRecord(None, Some("shell"), ACTION(
        jobu45tracker = jobTracker,
        nameu45node = nameNode,
        prepare = prepare,
        jobu45xml = jobXml,
        configuration = configBuilderImpl(configuration),
        exec = exec,
        argument = arguments,
        envu45var = environmentVariables,
        file = file,
        archive = archive,
        captureu45output = if (captureOutput) Some(FLAG()) else None,
        xmlns = "uri:oozie:shell-action:0.3")
      )
    }
  }
}
