package com.klout.scoozie.runner

import com.klout.scoozie.utils.ExecutionUtils
import com.klout.scoozie.utils.ExecutionUtils.{OozieSuccess, OozieError}

import scala.util.{Success, Failure, Try}

abstract class ScoozieApp(properties: Option[Map[String, String]]) extends App {
  val usage = "java -cp <...> com.your.scoozie.app.ObjectName -todayString=foo -yesterdayString=foo ..."

  val argumentProperties: Option[Map[String, String]] = {
    if (args nonEmpty) {
      Some(args.map(arg => {
        if (arg(0) != '-') throw new RuntimeException("error: usage is " + usage)
        ExecutionUtils.toProperty(arg.tail)
      }).toMap)
    } else None
  }

  val jobProperties = (argumentProperties ++ properties).reduceOption(_ ++ _)

  val writeResult: Try[Unit]
  val executionResult: Either[OozieError, OozieSuccess]

  def logWriteResult() = writeResult match {
    case Success(_) => println("Successfully wrote application.")
    case Failure(throwable) =>
      println(throwable.getMessage)
      throwable.printStackTrace()
  }
}