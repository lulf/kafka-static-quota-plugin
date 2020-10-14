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
import java.util.Collections
import java.{lang, util}

import org.apache.kafka.common.Cluster
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.security.auth.KafkaPrincipal


/**
 * Allows configuring a static quota for a broker using configuration.
 */
class StaticQuotaCallback extends ClientQuotaCallback {
  @volatile private var brokerId: String = ""
  @volatile private var quota: Quota = null

  override def configure(configs: util.Map[String, _]): Unit = {
    // println("Configuring quota callback")
    brokerId = configs.get("broker.id").toString
    val bound = lang.Double.parseDouble(configs.get("broker.quota").toString)
    quota = Quota.upperBound(bound)
  }

  override def quotaMetricTags(quotaType: ClientQuotaType, principal: KafkaPrincipal, clientId: String): util.Map[String, String] = {
   // println("quotaMetricTags. type: " + quotaType + ", principal: " + principal + ", clientId: " + clientId)
    // Return the same tag for all clients, which will cause throtting to be applied across all of them.
    Collections.singletonMap("broker.id", brokerId)
  }

  override def quotaLimit(quotaType: ClientQuotaType, metricTags: util.Map[String, String]): lang.Double = {
    // println("quotaLimit. type: " + quotaType + ", metricTags: " + metricTags)
    quota.bound
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

