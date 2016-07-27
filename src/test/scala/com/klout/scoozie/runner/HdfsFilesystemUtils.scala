package com.klout.scoozie.runner

import com.klout.scoozie.writer.FileSystemUtils
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.util.Try
import com.klout.scoozie.utils.TryImplicits._

class HdfsFilesystemUtils(fs: FileSystem) extends FileSystemUtils {
  override def writeTextFile(path: String, text: String): Try[Unit] = {
    for {
      out <- Try({
        println(s"Creating path: $path")
        fs.create(new Path(path))
      })
      _ <- Try({
        println(
          s"""Writing: $text
              |
             |To: $path
           """.stripMargin)
        out.write(text.getBytes("UTF-8"))
      }).doFinally(out.close())
    } yield ()
  }

  override def makeDirectory(path: String): Try[Unit] = {
    Try(fs.mkdirs(new Path(path)))
  }
}
