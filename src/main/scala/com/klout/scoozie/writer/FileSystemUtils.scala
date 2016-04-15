package com.klout.scoozie.writer

import scala.util.Try

trait FileSystemUtils {
  def writeTextFile(path: String, text: String): Try[Unit]

  def makeDirectory(path: String): Try[Unit]
}