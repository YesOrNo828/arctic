
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

package com.netease.arctic.trino.unkeyed;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.netease.arctic.data.DataFileType;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.delete.TrinoDeleteFile;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

/**
 * Iceberg original IcebergSplit has some problems for arctic, such as iceberg version, table type.
 */
public class IcebergSplit
    implements ConnectorSplit {
  private static final int INSTANCE_SIZE = ClassLayout.parseClass(IcebergSplit.class).instanceSize();

  private final String path;
  private final long start;
  private final long length;
  private final long fileSize;
  private final IcebergFileFormat fileFormat;
  private final List<HostAddress> addresses;
  private final String partitionSpecJson;
  private final String partitionDataJson;
  private final List<TrinoDeleteFile> deletes;
  private Long transactionId;
  private DataFileType fileType;

  @JsonCreator
  public IcebergSplit(
      @JsonProperty("path") String path,
      @JsonProperty("start") long start,
      @JsonProperty("length") long length,
      @JsonProperty("fileSize") long fileSize,
      @JsonProperty("fileFormat") IcebergFileFormat fileFormat,
      @JsonProperty("addresses") List<HostAddress> addresses,
      @JsonProperty("partitionSpecJson") String partitionSpecJson,
      @JsonProperty("partitionDataJson") String partitionDataJson,
      @JsonProperty("deletes") List<TrinoDeleteFile> deletes,
      @JsonProperty("transactionId") Long transactionId,
      @JsonProperty("fileType") DataFileType fileType) {
    this.path = requireNonNull(path, "path is null");
    this.start = start;
    this.length = length;
    this.fileSize = fileSize;
    this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
    this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    this.partitionSpecJson = requireNonNull(partitionSpecJson, "partitionSpecJson is null");
    this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
    this.deletes = ImmutableList.copyOf(requireNonNull(deletes, "deletes is null"));
    this.transactionId = transactionId;
    this.fileType = fileType;
  }

  @Override
  public boolean isRemotelyAccessible() {
    return true;
  }

  @JsonProperty
  @Override
  public List<HostAddress> getAddresses() {
    return addresses;
  }

  @JsonProperty
  public String getPath() {
    return path;
  }

  @JsonProperty
  public long getStart() {
    return start;
  }

  @JsonProperty
  public long getLength() {
    return length;
  }

  @JsonProperty
  public long getFileSize() {
    return fileSize;
  }

  @JsonProperty
  public IcebergFileFormat getFileFormat() {
    return fileFormat;
  }

  @JsonProperty
  public String getPartitionSpecJson() {
    return partitionSpecJson;
  }

  @JsonProperty
  public String getPartitionDataJson() {
    return partitionDataJson;
  }

  @JsonProperty
  public List<TrinoDeleteFile> getDeletes() {
    return deletes;
  }

  @JsonProperty
  public Long getTransactionId() {
    return transactionId;
  }

  @JsonProperty
  public DataFileType getFileType() {
    return fileType;
  }

  @Override
  public Object getInfo() {
    return ImmutableMap.builder()
        .put("path", path)
        .put("start", start)
        .put("length", length)
        .buildOrThrow();
  }

  @Override
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE +
        estimatedSizeOf(path) +
        estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes) +
        estimatedSizeOf(partitionSpecJson) +
        estimatedSizeOf(partitionDataJson) +
        estimatedSizeOf(deletes, TrinoDeleteFile::getRetainedSizeInBytes);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .addValue(path)
        .addValue(start)
        .addValue(length)
        .toString();
  }
}
