/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.catalog.lakehouse.iceberg.web.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.gravitino.catalog.lakehouse.iceberg.web.IcebergObjectMapper;
import org.apache.iceberg.metrics.MetricsReport;

public class IcebergMetricsFormatter {

  private ObjectMapper icebergObjectMapper;

  IcebergMetricsFormatter() {
    this.icebergObjectMapper = IcebergObjectMapper.getInstance();
  }

  public String toPrintableString(MetricsReport metricsReport) {
    try {
      return toJson(metricsReport);
    } catch (JsonProcessingException e) {
      return metricsReport.toString();
    }
  }

  public String toJson(MetricsReport metricsReport) throws JsonProcessingException {
    return icebergObjectMapper.writeValueAsString(metricsReport);
  }
}
