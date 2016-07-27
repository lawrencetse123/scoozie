package com.klout.scoozie.runner

import com.github.sakserv.minicluster.impl.OozieLocalServer
import org.apache.hadoop.conf.Configuration
import org.apache.oozie.client.OozieClient
import org.apache.oozie.local.LocalOozie

trait OozieClientProvider {
  val oozieClient: OozieClient
  def oozieCoordClient: OozieClient
}

trait TestOozieClientProvider extends OozieClientProvider {
  this: HdfsProvider =>

  lazy val oozieLocalServer = {
    val server = new OozieLocalServer.Builder()
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

    server.start()
    server
  }

  lazy val oozieClient: OozieClient = oozieLocalServer.getOozieClient
  def oozieCoordClient: OozieClient = {
    oozieLocalServer.getOozieClient
    LocalOozie.getCoordClient
  }
}
