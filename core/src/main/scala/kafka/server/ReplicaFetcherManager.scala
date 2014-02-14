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

package kafka.server

import kafka.cluster.Broker
import kafka.utils.Utils

trait ReplicaFetcherManagerMBean {
  def getMaxLag(): Long
}

object ReplicaFetcherManager {
  val MBeanName = "kafka.server:type=ReplicaFetcherManager,name=ReplicaOps"
}

class ReplicaFetcherManager(private val brokerConfig: KafkaConfig, private val replicaMgr: ReplicaManager)
        extends AbstractFetcherManager("ReplicaFetcherManager on broker " + brokerConfig.brokerId,
                                       "Replica", brokerConfig.numReplicaFetchers) with ReplicaFetcherManagerMBean {
  Utils.registerMBean(this, ReplicaFetcherManager.MBeanName)

  override def createFetcherThread(fetcherId: Int, sourceBroker: Broker): AbstractFetcherThread = {
    new ReplicaFetcherThread("ReplicaFetcherThread-%d-%d".format(fetcherId, sourceBroker.id), sourceBroker, brokerConfig, replicaMgr)
  }

  override def getMaxLag(): Long = {
    super.getMaxLag()
  }

  def shutdown() {
    info("shutting down")
    Utils.unregisterMBean(ReplicaFetcherManager.MBeanName)
    closeAllFetchers()
    info("shutdown completed")
  }
}
