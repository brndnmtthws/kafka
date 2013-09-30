/**
 *
 *
 *
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

package kafka.metrics

import com.yammer.metrics.Metrics
import java.net.InetSocketAddress
import com.yammer.metrics.reporting.GraphiteReporter
import java.util.concurrent.TimeUnit
import kafka.utils.{Utils, VerifiableProperties, Logging}


private trait KafkaGraphiteMetricsReporterMBean extends KafkaMetricsReporterMBean


private class KafkaGraphiteMetricsReporter extends KafkaMetricsReporter
with KafkaGraphiteMetricsReporterMBean
with Logging {

  private var underlying: GraphiteReporter = null
  private var running = false
  private var initialized = false
  private var address: InetSocketAddress = null
  private var graphiteHost: String = _
  private var graphitePort: Int = _
  private var graphitePrefix: String = _

  override def getMBeanName = "kafka:type=kafka.metrics.KafkaGraphiteMetricsReporter"

  override def init(props: VerifiableProperties) {
    synchronized {
      if (!initialized) {
        val metricsConfig = new KafkaMetricsConfig(props)
        graphiteHost = props.getString("kafka.graphite.metrics.host", "graphite_host")
        graphitePort = props.getInt("kafka.graphite.metrics.port", 2003)
        graphitePrefix = props.getString("kafka.graphite.metrics.prefix", "kafka")
        underlying = new GraphiteReporter(Metrics.defaultRegistry(), graphiteHost, graphitePort, graphitePrefix)
        if (props.getBoolean("kafka.graphite.metrics.reporter.enabled", default = false)) {
          initialized = true
          startReporter(metricsConfig.pollingIntervalSecs)
        }
      }
    }
  }

  override def startReporter(pollingPeriodSecs: Long) {
    synchronized {
      if (initialized && !running) {
        underlying.start(pollingPeriodSecs, TimeUnit.SECONDS)
        running = true
        info("Started Kafka Graphite metrics reporter with polling period %d seconds".format(pollingPeriodSecs))
      }
    }
  }


  override def stopReporter() {
    synchronized {
      if (initialized && running) {
        underlying.shutdown()
        running = false
        info("Stopped Kafka Graphite metrics reporter")
        underlying = new GraphiteReporter(Metrics.defaultRegistry(), graphiteHost, graphitePort, graphitePrefix)
      }
    }
  }

}