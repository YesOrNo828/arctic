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

package com.netease.arctic.flink.lookup;

import com.netease.arctic.flink.read.MixedIncrementalLoader;
import com.netease.arctic.flink.read.hybrid.enumerator.MergeOnReadIncrementalPlanner;
import com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunction;
import com.netease.arctic.flink.read.source.FlinkArcticMORDataReader;
import com.netease.arctic.flink.table.ArcticTableLoader;
import com.netease.arctic.table.ArcticTable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.io.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.netease.arctic.flink.table.descriptors.ArcticValidator.LOOKUP_RELOADING_INTERVAL_SECONDS;
import static com.netease.arctic.flink.util.ArcticUtils.loadArcticTable;
import static org.apache.flink.util.Preconditions.checkArgument;

public class ArcticLookupFunction extends TableFunction<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(ArcticLookupFunction.class);
  private ArcticTable arcticTable;
  private KVTable kvTable;
  private final List<String> joinKeys;
  private final long lruCacheSize;
  private final Schema projectSchema;
  private final List<Expression> filters;
  private final ArcticTableLoader loader;
  private boolean loading = false;
  private long nextLoadTime = Long.MIN_VALUE;
  private final long reloadIntervalSeconds;
  private MixedIncrementalLoader<RowData> incrementalLoader;
  private Configuration config;

  public ArcticLookupFunction(
      ArcticTable arcticTable,
      List<String> joinKeys,
      Schema projectSchema,
      long cacheMaxRows,
      List<Expression> filters,
      ArcticTableLoader tableLoader,
      Configuration config) {
    checkArgument(
        arcticTable.isKeyedTable(),
        String.format(
            "Only keyed arctic table support lookup join, this table [%s] is an unkeyed table.", arcticTable.name()));

    this.joinKeys = joinKeys;
    this.projectSchema = projectSchema;
    this.lruCacheSize = cacheMaxRows;
    this.filters = filters;
    this.loader = tableLoader;
    this.config = config;
    this.reloadIntervalSeconds = config.get(LOOKUP_RELOADING_INTERVAL_SECONDS);
  }

  @Override
  public void open(FunctionContext context) throws IOException {
    if (arcticTable == null) {
      arcticTable = loadArcticTable(loader).asKeyedTable();
    }
    arcticTable.refresh();

    LOG.info(
        "projected schema {}.\n table schema {}.",
        projectSchema,
        arcticTable.schema());
    kvTable = KVTable.create(
        new StateFactory(generateRocksDBPath(context, arcticTable.name())),
        arcticTable.asKeyedTable().primaryKeySpec().fieldNames(),
        joinKeys,
        lruCacheSize,
        projectSchema,
        config);
    kvTable.open();
    this.incrementalLoader =
        new MixedIncrementalLoader<>(
            new MergeOnReadIncrementalPlanner(loader),
            new FlinkArcticMORDataReader(
                arcticTable.io(),
                arcticTable.schema(),
                projectSchema,
                arcticTable.asKeyedTable().primaryKeySpec(),
                null,
                true,
                RowDataUtil::convertConstant,
                true
            ),
            new RowDataReaderFunction(
                new Configuration(),
                projectSchema,
                arcticTable.schema(),
                arcticTable.asKeyedTable().primaryKeySpec(),
                null,
                true,
                arcticTable.io(),
                true
            ),
            filters
        );
    checkAndLoad();
  }

  public void eval(Object... values) {
    try {
      checkAndLoad();
      RowData lookupKey = GenericRowData.of(values);
      List<RowData> results = kvTable.get(lookupKey);
      results.forEach(this::collect);
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    }
  }

  private synchronized void checkAndLoad() throws IOException {
    if (nextLoadTime > System.currentTimeMillis()) {
      return;
    }
    if (loading) {
      LOG.info("Mixed table incremental loader is running.");
      return;
    }
    nextLoadTime = System.currentTimeMillis() + 1000 * reloadIntervalSeconds;

    loading = true;
    long batchStart = System.currentTimeMillis();
    while (incrementalLoader.hasNext()) {
      long start = System.currentTimeMillis();
      try (CloseableIterator<RowData> iterator = incrementalLoader.next()) {
        if (kvTable.initialized()) {
          kvTable.upsert(iterator);
        } else {
          LOG.info("This table {} is still under initialization progress.", arcticTable.name());
          kvTable.initial(iterator);
        }
      }
      LOG.info("Split task fetched, cost {}ms.", System.currentTimeMillis() - start);
    }
    if (!kvTable.initialized()) {
      kvTable.waitWriteRocksDBCompleted();
    }
    LOG.info("{} table lookup loading, these batch tasks completed, cost {}ms.",
        arcticTable.name(), System.currentTimeMillis() - batchStart);
    loading = false;
  }

  @Override
  public void close() throws Exception {
    if (kvTable != null) {
      kvTable.close();
    }
  }

  private String generateRocksDBPath(FunctionContext context, String tableName) {
    String tmpPath = getTmpDirectoryFromTMContainer(context);
    File db = new File(tmpPath, tableName + "-lookup-" + UUID.randomUUID());
    return db.toString();
  }

  private static String getTmpDirectoryFromTMContainer(FunctionContext context) {
    try {
      Field field = context.getClass().getDeclaredField("context");
      field.setAccessible(true);
      StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) field.get(context);
      String[] tmpDirectories =
          runtimeContext.getTaskManagerRuntimeInfo().getTmpDirectories();
      return tmpDirectories[ThreadLocalRandom.current().nextInt(tmpDirectories.length)];
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
