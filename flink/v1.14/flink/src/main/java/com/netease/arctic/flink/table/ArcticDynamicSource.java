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

package com.netease.arctic.flink.table;

import com.netease.arctic.flink.lookup.ArcticLookupFunction;
import com.netease.arctic.flink.util.FilterUtil;
import com.netease.arctic.flink.util.IcebergAndFlinkFilters;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.utils.SchemaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Flink table api that generates source operators.
 */
public class ArcticDynamicSource implements ScanTableSource, SupportsFilterPushDown,
    SupportsProjectionPushDown, SupportsLimitPushDown, SupportsWatermarkPushDown, LookupTableSource {

  public static final Logger LOG = LoggerFactory.getLogger(ArcticDynamicSource.class);

  protected final String tableName;

  private final ScanTableSource arcticDynamicSource;
  private final ArcticTable arcticTable;
  private final Map<String, String> properties;

  private int[] projectFields;
  private List<Expression> filters;
  private ArcticTableLoader tableLoader;

  @Nullable
  protected WatermarkStrategy<RowData> watermarkStrategy;

  /**
   * @param tableName           tableName
   * @param arcticDynamicSource underlying source
   * @param arcticTable         arcticTable
   * @param properties          With all ArcticTable properties and sql options
   * @param tableLoader
   */
  public ArcticDynamicSource(String tableName,
                             ScanTableSource arcticDynamicSource,
                             ArcticTable arcticTable,
                             Map<String, String> properties,
                             ArcticTableLoader tableLoader) {
    this.tableName = tableName;
    this.arcticDynamicSource = arcticDynamicSource;
    this.arcticTable = arcticTable;
    this.properties = properties;
    this.tableLoader = tableLoader;
  }

  public ArcticDynamicSource(String tableName,
                             ScanTableSource arcticDynamicSource,
                             ArcticTable arcticTable,
                             Map<String, String> properties,
                             ArcticTableLoader tableLoader,
                             int[] projectFields,
                             List<Expression> filters) {
    this.tableName = tableName;
    this.arcticDynamicSource = arcticDynamicSource;
    this.arcticTable = arcticTable;
    this.properties = properties;
    this.tableLoader = tableLoader;
    this.projectFields = projectFields;
    this.filters = filters;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return arcticDynamicSource.getChangelogMode();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    ScanRuntimeProvider origin = arcticDynamicSource.getScanRuntimeProvider(scanContext);
    Preconditions.checkArgument(origin instanceof DataStreamScanProvider,
        "file or log ScanRuntimeProvider should be DataStreamScanProvider, but provided is " +
            origin.getClass());
    return origin;
  }

  @Override
  public DynamicTableSource copy() {
    return
        new ArcticDynamicSource(
            tableName, arcticDynamicSource, arcticTable, properties, tableLoader, projectFields, filters);
  }

  @Override
  public String asSummaryString() {
    return "Arctic Dynamic Source";
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    IcebergAndFlinkFilters icebergAndFlinkFilters = FilterUtil.convertFlinkExpressToIceberg(filters);
    this.filters = icebergAndFlinkFilters.expressions();

    if (arcticDynamicSource instanceof SupportsFilterPushDown) {
      return ((SupportsFilterPushDown) arcticDynamicSource).applyFilters(filters);
    } else {
      return Result.of(Collections.emptyList(), filters);
    }
  }

  @Override
  public boolean supportsNestedProjection() {
    if (arcticDynamicSource instanceof SupportsProjectionPushDown) {
      return ((SupportsProjectionPushDown) arcticDynamicSource).supportsNestedProjection();
    } else {
      return false;
    }
  }

  @Override
  public void applyProjection(int[][] projectedFields) {
    projectFields = new int[projectedFields.length];
    for (int i = 0; i < projectedFields.length; i++) {
      Preconditions.checkArgument(
          projectedFields[i].length == 1,
          "Don't support nested projection now.");
      projectFields[i] = projectedFields[i][0];
    }

    if (arcticDynamicSource instanceof SupportsProjectionPushDown) {
      ((SupportsProjectionPushDown) arcticDynamicSource).applyProjection(projectedFields);
    }
  }

  @Override
  public void applyLimit(long newLimit) {
    if (arcticDynamicSource instanceof SupportsLimitPushDown) {
      ((SupportsLimitPushDown) arcticDynamicSource).applyLimit(newLimit);
    }
  }

  @Override
  public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
    if (arcticDynamicSource instanceof SupportsWatermarkPushDown) {
      ((SupportsWatermarkPushDown) arcticDynamicSource).applyWatermark(watermarkStrategy);
    }
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    int[] joinKeys = new int[context.getKeys().length];
    for (int i = 0; i < context.getKeys().length; i++) {
      Preconditions.checkArgument(
          context.getKeys()[i].length == 1,
          "Arctic lookup join doesn't support the row field as a joining key.");
      joinKeys[i] = context.getKeys()[i][0];
    }

    return TableFunctionProvider.of(getLookupFunction(joinKeys));
  }

  private ArcticLookupFunction getLookupFunction(int[] joinKeys) {
    Schema arcticTableSchema = arcticTable.schema();

    Schema projectedSchema;
    if (projectFields == null) {
      LOG.info("projectFields is null.");
      projectedSchema = arcticTable.schema();
    } else {
      List<String> projectedFieldNames =
          Arrays
              .stream(projectFields)
              .mapToObj(
                  index ->
                      arcticTable.schema().columns().get(index).name())
              .collect(Collectors.toList());
      projectedSchema = SchemaUtil.convertFieldsToSchema(arcticTableSchema, projectedFieldNames);
      LOG.info(
          "projected schema {}.\n table schema {}.",
          projectedSchema,
          arcticTable.schema());
    }

    List<String> joinKeyNames =
        Arrays
            .stream(joinKeys)
            .mapToObj(
                index -> projectedSchema.columns().get(index).name())
            .collect(Collectors.toList());

    Configuration config = new Configuration();
    properties.forEach(config::setString);

    return
        new ArcticLookupFunction(
            arcticTable,
            joinKeyNames,
            projectedSchema,
            filters,
            tableLoader,
            config);
  }
}