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

package com.netease.arctic.io;

import com.netease.arctic.TableTestBase;
import com.netease.arctic.io.writer.GenericBaseTaskWriter;
import com.netease.arctic.io.writer.GenericChangeTaskWriter;
import com.netease.arctic.io.writer.GenericTaskWriters;
import com.netease.arctic.io.writer.SortedPosDeleteWriter;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

public class TaskWriterTest extends TableTestBase {

  @Test
  public void testBaseWriter() throws IOException {
    GenericBaseTaskWriter writer = GenericTaskWriters.builderFor(testKeyedTable)
        .withTransactionId(1).buildBaseWriter();

    for (Record record : writeRecords()) {
      writer.write(record);
    }
    WriteResult result = writer.complete();
    Assert.assertEquals(4, result.dataFiles().length);
  }

  @Test
  public void testBasePosDeleteWriter() throws IOException {
    SortedPosDeleteWriter<Record> writer = GenericTaskWriters.builderFor(testKeyedTable)
        .withTransactionId(1).buildBasePosDeleteWriter(2, 1, FILE_A.partition());

    writer.delete(FILE_A.path(), 1);
    writer.delete(FILE_A.path(), 3);
    writer.delete(FILE_A.path(), 5);
    List<DeleteFile> result = writer.complete();
    Assert.assertEquals(1, result.size());
  }

  @Test
  public void testChangeWriter() throws IOException {
    GenericChangeTaskWriter writer =   GenericTaskWriters.builderFor(testKeyedTable)
        .withTransactionId(1).buildChangeWriter();


    for (Record record : writeRecords()) {
      writer.write(record);
    }

    WriteResult result = writer.complete();
    Assert.assertEquals(4, result.dataFiles().length);
  }

  private List<Record> writeRecords() {
    GenericRecord record = GenericRecord.create(TABLE_SCHEMA);

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    builder.add(record.copy(ImmutableMap.of("id", 1, "name", "john", "op_time",
        LocalDateTime.of(2022, 1, 1, 1, 0, 0))));
    builder.add(record.copy(ImmutableMap.of("id", 2, "name", "lily", "op_time",
        LocalDateTime.of(2022, 1, 1, 12, 0, 0))));
    builder.add(record.copy(ImmutableMap.of("id", 3, "name", "jake", "op_time",
        LocalDateTime.of(2022, 1, 2, 23, 0, 0))));
    builder.add(record.copy(ImmutableMap.of("id", 4, "name", "sam", "op_time",
        LocalDateTime.of(2022, 1, 2, 6, 0, 0))));
    builder.add(record.copy(ImmutableMap.of("id", 5, "name", "john", "op_time",
        LocalDateTime.of(2022, 1, 1, 12, 0, 0))));

    return builder.build();
  }
}
