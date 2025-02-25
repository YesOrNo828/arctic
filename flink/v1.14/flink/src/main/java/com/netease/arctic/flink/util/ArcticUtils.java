/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.flink.util;

import com.netease.arctic.flink.shuffle.LogRecordV1;
import com.netease.arctic.flink.shuffle.ShuffleHelper;
import com.netease.arctic.flink.table.ArcticTableLoader;
import com.netease.arctic.flink.table.descriptors.ArcticValidator;
import com.netease.arctic.flink.write.ArcticLogWriter;
import com.netease.arctic.flink.write.MetricsGenerator;
import com.netease.arctic.flink.write.hidden.HiddenLogWriter;
import com.netease.arctic.flink.write.hidden.kafka.HiddenKafkaFactory;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.PrimaryKeySpec;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.IdGenerator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.netease.arctic.table.TableProperties.LOG_STORE_DATA_VERSION;
import static com.netease.arctic.table.TableProperties.LOG_STORE_DATA_VERSION_DEFAULT;

/**
 * An util that loads arctic table, build arctic log writer and so on.
 */
public class ArcticUtils {

  public static final Logger LOGGER = LoggerFactory.getLogger(ArcticUtils.class);

  public static ArcticTable loadArcticTable(ArcticTableLoader tableLoader) {
    tableLoader.open();
    ArcticTable table = tableLoader.loadArcticTable();
    try {
      tableLoader.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return table;
  }

  public static List<String> getPrimaryKeys(ArcticTable table) {
    if (table.isUnkeyedTable()) {
      return Collections.emptyList();
    }
    return ((KeyedTable) table).primaryKeySpec().fields()
        .stream()
        .map(PrimaryKeySpec.PrimaryKeyField::fieldName)
        .collect(Collectors.toList());
  }

  public static MetricsGenerator getMetricsGenerator(boolean metricsEventLatency, boolean metricsEnable,
                                                     ArcticTable arcticTable, RowType flinkSchemaRowType,
                                                     Schema writeSchema) {
    MetricsGenerator metricsGenerator;
    if (metricsEventLatency) {
      String modifyTimeColumn = arcticTable.properties().get(TableProperties.TABLE_EVENT_TIME_FIELD);
      metricsGenerator = MetricsGenerator.newGenerator(arcticTable.schema(), flinkSchemaRowType,
          modifyTimeColumn, metricsEnable);
    } else {
      metricsGenerator = MetricsGenerator.empty(metricsEnable);
    }
    return metricsGenerator;
  }

  public static boolean arcticWALWriterEnable(String arcticEmitMode) {
    return arcticEmitMode.contains(ArcticValidator.ARCTIC_EMIT_LOG);
  }

  /**
   * only when {@link ArcticValidator#ARCTIC_EMIT_MODE} contains {@link ArcticValidator#ARCTIC_EMIT_FILE}
   * and enable {@link TableProperties#ENABLE_LOG_STORE}
   * create logWriter according to {@link TableProperties#LOG_STORE_DATA_VERSION}
   *
   * @param properties
   * @param producerConfig
   * @param topic
   * @param tableSchema
   * @return
   */
  public static ArcticLogWriter buildArcticLogWriter(Map<String, String> properties,
                                                     Properties producerConfig, String topic, TableSchema tableSchema,
                                                     String arcticEmitMode,
                                                     ShuffleHelper helper) {
    if (!arcticWALWriterEnable(arcticEmitMode)) {
      return null;
    }

    boolean streamEnable = PropertyUtil.propertyAsBoolean(properties, TableProperties.ENABLE_LOG_STORE,
        TableProperties.ENABLE_LOG_STORE_DEFAULT);
    if (!streamEnable) {
      throw new ValidationException(
          "emit to kafka was set, but no kafka config be found, please set kafka config first");
    }

    String version = properties.getOrDefault(LOG_STORE_DATA_VERSION, LOG_STORE_DATA_VERSION_DEFAULT);
    if (LOG_STORE_DATA_VERSION_DEFAULT.equals(version)) {
      LOGGER.info("build log writer: HiddenLogWriter(v1)");
      return new HiddenLogWriter(
          FlinkSchemaUtil.convert(tableSchema),
          producerConfig,
          topic,
          new HiddenKafkaFactory<>(),
          LogRecordV1.fieldGetterFactory,
          IdGenerator.generateUpstreamId(),
          helper);
    }
    throw new UnsupportedOperationException("don't support log version '" + version +
        "'. only support 'v1' or empty");
  }

  public static boolean arcticFileWriterEnable(String arcticEmitMode) {
    return arcticEmitMode.contains(ArcticValidator.ARCTIC_EMIT_FILE);
  }

  public static boolean isToBase(boolean overwrite) {
    boolean toBase = overwrite;
    LOGGER.info("is write to base:{}", toBase);
    return toBase;
  }

}
