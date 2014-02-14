/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin


import joptsimple.OptionParser
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import javax.management.remote.{JMXServiceURL, JMXConnectorFactory}
import javax.management.JMX
import javax.management.ObjectName
import scala.Some
import kafka.common.{TopicAndPartition, BrokerNotAvailableException}
import kafka.server.{ReplicaFetcherManager, ReplicaFetcherManagerMBean}


object WaitForReplication extends Logging {

  private case class BrokerParams(zkConnect: String, brokerId: java.lang.Integer)

  private def invokeTest(params: BrokerParams): Boolean = {
    var zkClient: ZkClient = null
    try {
      zkClient = new ZkClient(params.zkConnect, 30000, 30000, ZKStringSerializer)
      ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + params.brokerId)._1 match {
        case Some(zkInfo) =>
          var brokerHost: String = null
          var brokerJmxPort: Int = -1
          try {
            Json.parseFull(zkInfo) match {
              case Some(m) =>
                val brokerInfo = m.asInstanceOf[Map[String, Any]]
                brokerHost = brokerInfo.get("host").get.toString
                brokerJmxPort = brokerInfo.get("jmx_port").get.asInstanceOf[Int]
              case None =>
                throw new BrokerNotAvailableException("Broker id %d does not exist".format(params.brokerId))
            }
          }
          val jmxUrl = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi".format(brokerHost, brokerJmxPort))
          info("Connecting to jmx url " + jmxUrl)
          val jmxc = JMXConnectorFactory.connect(jmxUrl, null)
          val mbsc = jmxc.getMBeanServerConnection
          val proxy = JMX.newMBeanProxy(mbsc,
              new ObjectName(ReplicaFetcherManager.MBeanName),
              classOf[ReplicaFetcherManagerMBean])

          val maxLag = proxy.getMaxLag
          println(s"Max lag is currently ${maxLag}")
          maxLag == 0
        case None =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(params.brokerId))
      }
    } catch {
      case t: Throwable =>
        error("Operation failed due to broker failure", t)
        false
    } finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  def main(args: Array[String]) {
    val parser = new OptionParser
    val brokerOpt = parser.accepts("broker", "REQUIRED: The broker to wait for.")
            .withRequiredArg
            .describedAs("Broker Id")
            .ofType(classOf[java.lang.Integer])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
            "Multiple URLS can be given to allow fail-over.")
            .withRequiredArg
            .describedAs("urls")
            .ofType(classOf[String])
    val numRetriesOpt = parser.accepts("num.retries", "Number of attempts to retry if shutdown does not complete.")
            .withRequiredArg
            .describedAs("number of retries")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(10)
    val retryIntervalOpt = parser.accepts("retry.interval.ms", "Retry interval if retries requested.")
            .withRequiredArg
            .describedAs("retry interval in ms (> 1000)")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(10000)

    val options = parser.parse(args : _*)
    CommandLineUtils.checkRequiredArgs(parser, options, brokerOpt, zkConnectOpt)

    val retryIntervalMs = options.valueOf(retryIntervalOpt).intValue.max(1000)
    val numRetries = options.valueOf(numRetriesOpt).intValue

    val brokerParams = BrokerParams(options.valueOf(zkConnectOpt), options.valueOf(brokerOpt))

    if (!invokeTest(brokerParams)) {
      (1 to numRetries).takeWhile(attempt => {
        info("Retry " + attempt)
        try {
          Thread.sleep(retryIntervalMs)
        }
        catch {
          case ie: InterruptedException => // ignore
        }
        !invokeTest(brokerParams)
      })
    }
  }

}

