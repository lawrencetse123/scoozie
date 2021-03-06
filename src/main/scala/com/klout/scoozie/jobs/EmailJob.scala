package com.klout.scoozie.jobs

import com.klout.scoozie.dsl.Job
import oozie.email_0_2._

import scalaxb.DataRecord

object EmailJob {
  def apply(jobName: String,
            to: String,
            subject: String,
            body: String,
            cc: Option[String] = None,
            bcc: Option[String] = None,
            contentType: Option[String] = None,
            attachment: Option[String] = None): Job[ACTION] = v0_1(
    jobName,
    to,
    subject,
    body,
    cc,
    bcc,
    contentType,
    attachment
  )

  def v0_1(jobName: String,
           to: String,
           subject: String,
           body: String,
           cc: Option[String] = None,
           bcc: Option[String] = None,
           contentType: Option[String] = None,
           attachment: Option[String] = None): Job[ACTION] = {

    val _jobName = jobName

    new Job[ACTION] {
      override val jobName = _jobName
      override val record: DataRecord[ACTION] = DataRecord(None, Some("email"), ACTION(
        to = to,
        cc = cc,
        bcc = bcc,
        subject = subject,
        body = body,
        content_type = contentType,
        attachment = attachment,
        xmlns = "uri:oozie:email-action:0.1")
      )
    }
  }
}