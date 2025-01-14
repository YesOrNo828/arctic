/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.netease.arctic.trino.keyed;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.netease.arctic.data.PrimaryKeyedFile;
import com.netease.arctic.io.reader.ArcticDeleteFilter;
import com.netease.arctic.scan.ArcticFileScanTask;
import com.netease.arctic.trino.unkeyed.IcebergPageSourceProvider;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.iceberg.FileIoProvider;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.delete.TrinoRow;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;

import java.util.List;
import java.util.stream.Collectors;

/**
 * ConnectorPageSourceProvider for Keyed Table
 */
public class KeyedPageSourceProvider implements ConnectorPageSourceProvider {

  private final IcebergPageSourceProvider icebergPageSourceProvider;
  private final TypeManager typeManager;
  private final FileIoProvider fileIoProvider;

  @Inject
  public KeyedPageSourceProvider(
      IcebergPageSourceProvider icebergPageSourceProvider,
      TypeManager typeManager,
      FileIoProvider fileIoProvider) {
    this.icebergPageSourceProvider = icebergPageSourceProvider;
    this.typeManager = typeManager;
    this.fileIoProvider = fileIoProvider;
  }

  @Override
  public ConnectorPageSource createPageSource(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<ColumnHandle> columns,
      DynamicFilter dynamicFilter) {
    KeyedConnectorSplit keyedConnectorSplit = (KeyedConnectorSplit) split;
    KeyedTableHandle keyedTableHandle = (KeyedTableHandle) table;
    List<IcebergColumnHandle> icebergColumnHandles = columns.stream().map(IcebergColumnHandle.class::cast)
        .collect(Collectors.toList());

    List<PrimaryKeyedFile> equDeleteFiles = keyedConnectorSplit.getKeyedTableScanTask().arcticEquityDeletes().stream()
        .map(ArcticFileScanTask::file).collect(Collectors.toList());
    Schema tableSchema = SchemaParser.fromJson(keyedTableHandle.getIcebergTableHandle().getTableSchemaJson());
    List<IcebergColumnHandle> deleteFilterRequiredSchema = IcebergUtil.getColumns(new KeyedDeleteFilter(
        equDeleteFiles,
        tableSchema,
        ImmutableList.of(),
        keyedTableHandle.getPrimaryKeySpec(),
        fileIoProvider.createFileIo(new HdfsEnvironment.HdfsContext(session), null)
    ).requiredSchema(), typeManager);
    ImmutableList.Builder<IcebergColumnHandle> requiredColumnsBuilder = ImmutableList.builder();
    requiredColumnsBuilder.addAll(icebergColumnHandles);
    deleteFilterRequiredSchema.stream()
        .filter(column -> !columns.contains(column))
        .forEach(requiredColumnsBuilder::add);
    List<IcebergColumnHandle> requiredColumns = requiredColumnsBuilder.build();
    ArcticDeleteFilter<TrinoRow> arcticDeleteFilter = new KeyedDeleteFilter(
        equDeleteFiles,
        tableSchema,
        requiredColumns,
        keyedTableHandle.getPrimaryKeySpec(),
        fileIoProvider.createFileIo(new HdfsEnvironment.HdfsContext(session), session.getQueryId())
    );

    return new KeyedConnectorPageSource(
        icebergColumnHandles,
        requiredColumns,
        icebergPageSourceProvider,
        transaction,
        session,
        keyedConnectorSplit,
        keyedTableHandle,
        dynamicFilter,
        typeManager,
        fileIoProvider,
        arcticDeleteFilter
    );
  }
}
