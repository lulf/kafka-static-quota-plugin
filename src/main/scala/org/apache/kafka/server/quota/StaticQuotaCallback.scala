/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.quota
import java.{lang, util}

import org.apache.kafka.common.Cluster
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.security.auth.KafkaPrincipal

class StaticQuotaConfig(val props: java.util.Map[_, _], doLog: Boolean)
  extends AbstractConfig(StaticQuotaConfig.configDef, props, doLog) {
}

/**
 * Allows configuring a static quota for a broker using configuration.
 */
class StaticQuotaCallback extends ClientQuotaCallback {
  @volatile private var brokerId: String = ""
  @volatile private var quotaMap: util.Map[ClientQuotaType, Quota] = null

  override def configure(configs: util.Map[String, _]): Unit = {
    // println("Configuring quota callback")
    val config = new StaticQuotaConfig(configs, true)

    brokerId = config.getString(StaticQuotaConfig.BrokerIdProp)

    val produceBound = config.getDouble(StaticQuotaConfig.ProduceQuotaProp)
    val fetchBound = config.getDouble(StaticQuotaConfig.FetchQuotaProp)
    val requestBound = config.getDouble(StaticQuotaConfig.RequestQuotaProp)

    val m = new util.HashMap[ClientQuotaType, Quota]()

    m.put(ClientQuotaType.PRODUCE, Quota.upperBound(produceBound))
    m.put(ClientQuotaType.FETCH, Quota.upperBound(fetchBound))
    m.put(ClientQuotaType.REQUEST, Quota.upperBound(requestBound))
    quotaMap = m
  }

  override def quotaMetricTags(quotaType: ClientQuotaType, principal: KafkaPrincipal, clientId: String): util.Map[String, String] = {
//    println("quotaMetricTags. type: " + quotaType + ", principal: " + principal + ", clientId: " + clientId)
    // Return the same tag for all clients, which will cause throtting to be applied across all of them.
    val m = new util.HashMap[String, String]()
    m.put("broker.id", brokerId)
    m.put("quota.type", quotaType.name());
    m
  }

  override def quotaLimit(quotaType: ClientQuotaType, metricTags: util.Map[String, String]): lang.Double = {
    // println("quotaLimit. type: " + quotaType + ", metricTags: " + metricTags)
    quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MaxValue)).bound()
  }

  override def updateClusterMetadata(cluster: Cluster): Boolean = false

  override def updateQuota(quotaType: ClientQuotaType, entity: ClientQuotaEntity, newValue: Double): Unit = {
    // println("Update quota. type: " + quotaType + ", entity: " + entity + ", newValue: " + newValue)
  }

  override def removeQuota(quotaType: ClientQuotaType, entity: ClientQuotaEntity): Unit = {
    // println("Remove quota. type: " + quotaType + ", entity: " + entity)
  }

  override def quotaResetRequired(quotaType: ClientQuotaType): Boolean = false

  override def close(): Unit = {}
}

object StaticQuotaConfig {
  val BrokerIdProp = "broker.id"
  val ProduceQuotaProp = "broker.quota.produce"
  val FetchQuotaProp = "broker.quota.fetch"
  val RequestQuotaProp = "broker.quota.request"

  private val configDef = {
    import ConfigDef.Importance._
    import ConfigDef.Type._

    new ConfigDef()
      .define(BrokerIdProp, STRING, HIGH, "Broker id")
      .define(ProduceQuotaProp, DOUBLE, Double.MaxValue, HIGH, "Produce quota")
      .define(FetchQuotaProp, DOUBLE, Double.MaxValue, HIGH, "Fetch quota")
      .define(RequestQuotaProp, DOUBLE, Double.MaxValue, HIGH, "Request quota")
  }
}
