package com.shorrockin.cascal

import org.apache.cassandra.thrift.CassandraDaemon
import org.apache.cassandra.config.DatabaseDescriptor
import java.io.File
import scala.collection.JavaConversions._
import java.util.{Collection => JCollection}
import java.net.ConnectException
import org.apache.thrift.transport.{TTransportException, TSocket}
import com.shorrockin.cascal.session._
import com.shorrockin.cascal.utils.{Utils, Logging}
import org.apache.cassandra.config.{CFMetaData, KSMetaData}

/**
 * trait which mixes in the functionality necessary to embed
 * cassandra into a unit test
 */
trait CassandraTestPool extends Logging {
  def borrow(f:(Session) => Unit) = {
    EmbeddedTestCassandra.init
    EmbeddedTestCassandra.pool.borrow(f)
  }
}

/**
 * maintains the single instance of the cassandra server
 */
object EmbeddedTestCassandra extends Logging with Schema {
  import Utils._
  var initialized = false

  val port = 9162
  val host = "localhost"
  val hosts  = Host(host, port, 250) :: Nil
  val params = new PoolParams(10, ExhaustionPolicy.Fail, 500L, 6, 2)
  lazy val pool = new SessionPool(hosts, params, Consistency.One)

  def init = synchronized {
    if (!initialized) {
      val homeDirectory = new File("target/cassandra.home.unit-tests")
      delete(homeDirectory)
      homeDirectory.mkdirs

      log.debug("creating cassandra instance at: " + homeDirectory.getCanonicalPath)
      log.debug("copying cassandra configuration files to root directory")

      val fileSep = System.getProperty("file.separator")
      val storageFile = new File(homeDirectory, "cassandra.yaml")
      val logFile = new File(homeDirectory, "log4j.properties")

      replace(copy(resource("/cassandra.yaml"), storageFile), ("%temp-dir%" -> (homeDirectory.getCanonicalPath + fileSep)))
      copy(resource("/log4j.properties"), logFile)

      System.setProperty("cassandra.config", toURI(homeDirectory.getCanonicalPath + fileSep + "cassandra.yaml").toString)
      System.setProperty("log4j.configuration", toURI(homeDirectory.getCanonicalPath + fileSep + "log4j.properties").toString);
      System.setProperty("cassandra-foreground","true");

      log.debug("creating data file and log location directories")
      DatabaseDescriptor.getAllDataFileLocations.foreach { (file) => new File(file).mkdirs }
      
      loadSchema
      
      val daemon = new CassandraDaemonThread
      daemon.start

      // try to make sockets until the server opens up - there has to be a better
      // way - just not sure what it is.
      val socket = new TSocket(host, port);
      var opened = false
      while (!opened) {
        try {
          socket.open()
          opened = true
          socket.close()
        } catch {
          case e:TTransportException => /* ignore */
          case e:ConnectException => /* ignore */
        }
      }

      initialized = true
    }
  }
    
  private def resource(str:String) = classOf[CassandraTestPool].getResourceAsStream(str)
}

/**
 * daemon thread used to start and stop cassandra
 */
class CassandraDaemonThread extends Thread("CassandraDaemonThread") with Logging {
  private val daemon = new CassandraDaemon

  setDaemon(true)

  /**
   * starts the server and blocks until it has
   * completed booting up.
   */
  def startServer = {

  }

  override def run:Unit = {
    log.debug("initializing cassandra daemon")
    daemon.init(new Array[String](0))
    log.debug("starting cassandra daemon")
    daemon.start
  }

  def close():Unit = {
    log.debug("instructing cassandra deamon to shut down")
    daemon.stop
    log.debug("blocking on cassandra shutdown")
    this.join
    log.debug("cassandra shut down")
  }
}

