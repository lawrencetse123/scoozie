package com.klout.scoozie.runner

import com.github.sakserv.minicluster.impl.OozieLocalServer
import org.apache.hadoop.conf.Configuration
import org.apache.oozie.client.OozieClient
import org.apache.oozie.local.LocalOozie

trait OozieClientProvider {
  val oozieClient: OozieClient
  def oozieCoordClient: OozieClient
  def startOozie(): Unit
  def stopOozie(): Unit
}

trait TestOozieClientProvider extends OozieClientProvider {
  this: TestHdfsProvider =>

  lazy val oozieLocalServer = new OozieLocalServer.Builder()
      .setOozieTestDir("embedded_oozie")
      .setOozieHomeDir("oozie_home")
      .setOozieUsername(System.getProperty("user.name"))
      .setOozieGroupname("testgroup")
      .setOozieYarnResourceManagerAddress("localhost")
      .setOozieHdfsDefaultFs(hdfsUri)
      .setOozieConf(new Configuration())
      .setOozieHdfsShareLibDir("/tmp/oozie_share_lib")
      .setOozieShareLibCreate(true)
      .setOozieLocalShareLibCacheDir("share_lib_cache")
      .setOoziePurgeLocalShareLibCache(false)
      .build()

  lazy val oozieClient: OozieClient = oozieLocalServer.getOozieClient

  def oozieCoordClient: OozieClient = {
    LocalOozie.getCoordClient
  }

  def startOozie(): Unit = oozieLocalServer.start()

  def stopOozie(): Unit = oozieLocalServer.stop()
}
