package com.klout.scoozie.runner

import com.klout.scoozie.utils.ExecutionUtils

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
}