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

package com.netease.arctic.utils;

import com.netease.arctic.iceberg.optimize.InternalRecordWrapper;
import com.netease.arctic.table.ArcticTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import java.util.List;

public class SnapshotFileUtil {
  public static void getSnapshotFiles(ArcticTable table, Snapshot snapshot,
                                      List<com.netease.arctic.ams.api.DataFile> addFiles,
                                      List<com.netease.arctic.ams.api.DataFile> deleteFiles) {
    Preconditions.checkNotNull(addFiles, "Add files to delete can not be null");
    Preconditions.checkNotNull(deleteFiles, "Delete files to delete can not be null");

    for (DataFile file : snapshot.addedFiles()) {
      addFiles.add(ConvertStructUtil.convertToAmsDatafile(file, table));
    }
    for (DataFile file : snapshot.deletedFiles()) {
      deleteFiles.add(ConvertStructUtil.convertToAmsDatafile(file, table));
    }

    table.io().doAs(() -> {
      getDeleteFiles(table, snapshot, addFiles, deleteFiles);
      return null;
    });
  }

  private static void getDeleteFiles(ArcticTable table, Snapshot snapshot,
                                     List<com.netease.arctic.ams.api.DataFile> addFiles,
                                     List<com.netease.arctic.ams.api.DataFile> deleteFiles) {
    HadoopTables tables = new HadoopTables();
    Table entriesTable = tables.load(table.location() + "#ENTRIES");
    IcebergGenerics.read(entriesTable)
        .where(Expressions.equal(ManifestEntryFields.SNAPSHOT_ID.name(), snapshot.snapshotId()))
        .build()
        .forEach(record -> {
          int status = (int) record.get(ManifestEntryFields.STATUS.fieldId());
          GenericRecord dataFile = (GenericRecord) record.get(ManifestEntryFields.DATA_FILE_ID);
          Integer contentId = (Integer) dataFile.getField(DataFile.CONTENT.name());
          if (contentId != null && contentId != 0) {
            String filePath = (String) dataFile.getField(DataFile.FILE_PATH.name());
            String partitionPath = null;
            GenericRecord parRecord = (GenericRecord) dataFile.getField(DataFile.PARTITION_NAME);
            if (parRecord != null) {
              InternalRecordWrapper wrapper = new InternalRecordWrapper(parRecord.struct());
              partitionPath = table.spec().partitionToPath(wrapper.wrap(parRecord));
            }
            Long fileSize = (Long) dataFile.getField(DataFile.FILE_SIZE.name());
            Long recordCount = (Long) dataFile.getField(DataFile.RECORD_COUNT.name());
            DeleteFile deleteFile;
            if (table.spec().isUnpartitioned()) {
              deleteFile = FileMetadata.deleteFileBuilder(table.spec())
                  .ofPositionDeletes()
                  .withPath(filePath)
                  .withFileSizeInBytes(fileSize)
                  .withRecordCount(recordCount)
                  .build();
            } else {
              deleteFile = FileMetadata.deleteFileBuilder(table.spec())
                  .ofPositionDeletes()
                  .withPath(filePath)
                  .withFileSizeInBytes(fileSize)
                  .withRecordCount(recordCount)
                  .withPartitionPath(partitionPath)
                  .build();
            }
            if (status == ManifestEntryFields.Status.DELETED.id()) {
              deleteFiles.add(ConvertStructUtil.convertToAmsDatafile(deleteFile, table));
            } else if (status == ManifestEntryFields.Status.ADDED.id()) {
              addFiles.add(ConvertStructUtil.convertToAmsDatafile(deleteFile, table));
            }
          }
        });
  }
}
