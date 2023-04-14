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


import com.netease.arctic.utils.SchemaUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.netease.arctic.flink.table.descriptors.ArcticValidator.ROCKSDB_WRITING_THREADS;

public class KVTableTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  @Rule
  public TestName name = new TestName();
  private Configuration config = new Configuration();
  private List<String> primaryKeys = Lists.newArrayList("id", "grade");

  private final Schema arcticSchema = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "grade", Types.StringType.get()),
      Types.NestedField.required(3, "num", Types.IntegerType.get()));

  private String dbPath;

  @Before
  public void before() throws IOException {
    dbPath = temp.newFolder().getPath();
  }

  @Test
  public void testRowDataSerializer() throws IOException {
    BinaryRowDataSerializer binaryRowDataSerializer = new BinaryRowDataSerializer(3);

    GenericRowData genericRowData = (GenericRowData) row(1, "2", 3);
    RowType rowType = FlinkSchemaUtil.convert(arcticSchema);
    RowDataSerializer rowDataSerializer = new RowDataSerializer(rowType);
    BinaryRowData record = rowDataSerializer.toBinaryRow(genericRowData);

    DataOutputSerializer view = new DataOutputSerializer(32);
    binaryRowDataSerializer.serialize(record, view);
    System.out.println(Arrays.toString(view.getCopyOfBuffer()));

    BinaryRowData desRowData = binaryRowDataSerializer.deserialize(new DataInputDeserializer(view.getCopyOfBuffer()));
    Assert.assertNotNull(desRowData);
    Assert.assertEquals(record.getInt(0), desRowData.getInt(0));
    Assert.assertEquals(record.getInt(1), desRowData.getInt(1));
    Assert.assertEquals(record.getInt(2), desRowData.getInt(2));

    // test join key rowData
    binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    List<String> keys = Lists.newArrayList("id", "grade");
    Schema keySchema = SchemaUtil.convertFieldsToSchema(arcticSchema, keys);
    rowType = FlinkSchemaUtil.convert(keySchema);
    rowDataSerializer = new RowDataSerializer(rowType);
    KeyRowData keyRowData = new KeyRowData(new int[]{0, 1}, row(2, "3", 4));
    KeyRowData keyRowData1 = new KeyRowData(new int[]{0, 1}, row(2, "3", 4));

    BinaryRowData binaryRowData = rowDataSerializer.toBinaryRow(keyRowData);
    view.clear();
    binaryRowDataSerializer.serialize(binaryRowData, view);
    byte[] rowBytes = view.getCopyOfBuffer();

    BinaryRowData binaryRowData1 = rowDataSerializer.toBinaryRow(keyRowData1);
    view.clear();
    binaryRowDataSerializer.serialize(binaryRowData1, view);
    byte[] rowBytes1 = view.getCopyOfBuffer();
    Assert.assertArrayEquals(rowBytes1, rowBytes);

  }

  @Test
  public void testInitialUniqueKeyTable() throws IOException {
    config.setInteger(ROCKSDB_WRITING_THREADS, 5);
    List<String> joinKeys = Lists.newArrayList("id", "grade");
    try (UniqueIndexTable uniqueIndexTable = (UniqueIndexTable) createTable(joinKeys)) {
      uniqueIndexTable.open();

      // During the initialization phase, the Merge-on-Read approach is used to retrieve data,
      // which will only return INSERT data.
      // When there are multiple entries with the same primary key, only one entry will be returned.
      initTable(
          uniqueIndexTable,
          upsertStream(
              row(RowKind.INSERT, 1, "1", 1),
              row(RowKind.INSERT, 2, "2", 2),
              row(RowKind.INSERT, 2, "3", 3),
              row(RowKind.INSERT, 2, "4", 4),
              row(RowKind.INSERT, 2, "5", 5)));

      if (!uniqueIndexTable.initialized()) {
        uniqueIndexTable.waitWriteRocksDBCompleted();
      }

      assertTable(
          uniqueIndexTable,
          row(1, "1"), row(1, "1", 1),
          row(2, "2"), row(2, "2", 2),
          row(2, "3"), row(2, "3", 3),
          row(2, "4"), row(2, "4", 4),
          row(2, "5"), row(2, "5", 5)
      );

      // upsert table
      upsertTable(
          uniqueIndexTable,
          upsertStream(
              row(RowKind.DELETE, 1, "1", 1),
              row(RowKind.INSERT, 2, "2", 2),
              row(RowKind.DELETE, 2, "2", 2),
              row(RowKind.UPDATE_BEFORE, 3, "3", 4),
              row(RowKind.UPDATE_AFTER, 3, "3", 5),
              row(RowKind.INSERT, 4, "4", 4)));

      assertTable(uniqueIndexTable,
          row(1, "1"), null,
          row(2, "2"), null,
          row(3, "3"), row(3, "3", 5),
          row(4, "4"), row(4, "4", 4));
    }
  }

  @Test
  public void testInitialSecondaryKeyTable() throws IOException {
    // primary keys are id and grade.
    List<String> joinKeys = Lists.newArrayList("id");
    try (SecondaryIndexTable secondaryIndexTable = (SecondaryIndexTable) createTable(joinKeys)) {
      secondaryIndexTable.open();

      initTable(
          secondaryIndexTable,
          upsertStream(
              row(RowKind.INSERT, 1, "1", 1),
              row(RowKind.INSERT, 2, "2", 2),
              row(RowKind.INSERT, 2, "3", 3),
              row(RowKind.INSERT, 2, "4", 4),
              row(RowKind.INSERT, 2, "5", 5)));

      if (!secondaryIndexTable.initialized()) {
        secondaryIndexTable.waitWriteRocksDBCompleted();
      }

      assertTableSet(
          secondaryIndexTable,
          row(1), row(1, "1", 1));
      assertTableSet(
          secondaryIndexTable,
          row(2),
          row(2, "2", 2),
          row(2, "3", 3),
          row(2, "4", 4),
          row(2, "5", 5));

      upsertTable(
          secondaryIndexTable,
          upsertStream(
              row(RowKind.DELETE, 1, "1", 1),
              row(RowKind.INSERT, 2, "2", 2),
              row(RowKind.DELETE, 2, "2", 2),
              row(RowKind.UPDATE_BEFORE, 3, "3", 4),
              row(RowKind.UPDATE_AFTER, 3, "3", 5),
              row(RowKind.INSERT, 3, "4", 4)));


      assertTableSet(
          secondaryIndexTable,
          row(1), null);
      assertTableSet(
          secondaryIndexTable,
          row(2), row(2, "3", 3), row(2, "4", 4), row(2, "5", 5));
      assertTableSet(
          secondaryIndexTable,
          row(3), row(3, "3", 5), row(3, "4", 4));
    }
  }

  @Test
  public void testUniqueKeyTable() throws IOException {
    List<String> joinKeys = Lists.newArrayList("id", "grade");
    try (UniqueIndexTable uniqueIndexTable =
             (UniqueIndexTable) KVTable.create(
                 new StateFactory(dbPath),
                 primaryKeys,
                 joinKeys,
                 2,
                 arcticSchema,
                 config)) {
      RowData expected = row(1, "2", 3);
      upsertTable(uniqueIndexTable, upsertStream(expected), row(1, "2"), expected);

      expected = row(1, "2", 4);
      upsertTable(uniqueIndexTable, upsertStream(expected), row(1, "2"), expected);

      upsertTable(
          uniqueIndexTable,
          upsertStream(
              row(RowKind.INSERT, 2, "3", 4),
              row(RowKind.DELETE, 2, "3", 4),
              row(RowKind.UPDATE_BEFORE, 1, "2", 4),
              row(RowKind.UPDATE_AFTER, 1, "2", 6)),
          row(2, "3"),
          null,
          row(1, "2"),
          row(1, "2", 6));
    }
  }

  @Test
  public void testSecondaryIndexTable() throws IOException {
    dbPath = temp.newFolder().getPath();

    List<String> joinKeys = Lists.newArrayList("id");

    try (SecondaryIndexTable secondaryIndexTable =
             (SecondaryIndexTable) KVTable.create(
                 new StateFactory(dbPath),
                 primaryKeys,
                 joinKeys,
                 2,
                 arcticSchema,
                 config)) {
      RowData expected = row(1, "2", 3);
      upsertTable(secondaryIndexTable, upsertStream(expected), row(1), expected);
      upsertTable(secondaryIndexTable, null, row(1), expected);

      expected = row(1, "2", 4);
      upsertTable(secondaryIndexTable, upsertStream(expected), row(1), expected);

      upsertTable(
          secondaryIndexTable,
          upsertStream(
              row(RowKind.INSERT, 2, "3", 4),
              row(RowKind.DELETE, 2, "3", 4),
              row(RowKind.UPDATE_BEFORE, 1, "2", 4),
              row(RowKind.UPDATE_AFTER, 1, "2", 6)),
          row(2),
          null,
          row(1),
          row(1, "2", 6)
      );
    }
  }

  private KVTable createTable(List<String> joinKeys) {
    return KVTable.create(
        new StateFactory(dbPath),
        primaryKeys,
        joinKeys,
        2,
        arcticSchema,
        config);
  }

  private void initTable(
      KVTable table, Iterator<RowData> initStream) throws IOException {
    if (initStream != null) {
      table.initial(initStream);
    }
  }

  private void upsertTable(
      KVTable table, Iterator<RowData> upsertStream, RowData... rows) throws IOException {
    if (upsertStream != null) {
      table.upsert(upsertStream);
    }
  }

  private void assertTable(KVTable table, RowData... rows) throws IOException {
    // Loop through the rows array in steps of 2
    for (int i = 0; i < rows.length; i = i + 2) {
      // Get the key and expected value at the current index and the next index
      RowData key = rows[i], expected = rows[i + 1];

      List<RowData> values = table.get(key);
      Assert.assertNotNull(values);
      if (expected == null) {
        Assert.assertEquals(0, values.size());
        continue;
      }
      Assert.assertEquals(expected.toString(), 1, values.size());
      RowData actual = values.get(0);
      assertRecord(expected, actual);
    }
  }

  private void assertTableSet(KVTable table, RowData key, RowData... expects) throws IOException {
    List<RowData> values = table.get(key);
    if (expects == null) {
      Assert.assertEquals(0, values.size());
      return;
    }
    for (int i = 0; i < expects.length; i = i + 1) {
      // Get the key and expected value at the current index and the next index
      RowData expected = expects[i];

      RowData actual = values.get(i);
      assertRecord(expected, actual);
    }
  }

  private void assertRecord(RowData expected, RowData actual) {
    if (!(actual instanceof BinaryRowData)) {
      throw new IllegalArgumentException("Only support BinaryRowData");
    }
    BinaryRowData binaryRowData = (BinaryRowData) actual;
    for (int j = 0; j < binaryRowData.getArity(); j++) {
      switch (j) {
        case 0:
        case 2:
          Assert.assertEquals(
              String.format("expected:%s, actual:%s.", expected.toString(), actual),
              expected.getInt(j),
              binaryRowData.getInt(j));
          break;
        case 1:
          Assert.assertEquals(
              String.format("expected:%s, actual:%s.", expected, actual),
              expected.getString(j),
              binaryRowData.getString(j));
          break;
      }
    }
  }

  RowData row(RowKind rowKind, Object... objects) {
    return GenericRowData.ofKind(rowKind, wrapStringData(objects));
  }

  RowData row(Object... objects) {
    return GenericRowData.of(wrapStringData(objects));
  }

  Object[] wrapStringData(Object... objects) {
    for (int i = 0; i < objects.length; i++) {
      if (objects[i] instanceof String) {
        objects[i] = StringData.fromString(objects[i].toString());
      }
    }
    return objects;
  }

  Iterator<RowData> upsertStream(RowData... rows) {
    return Lists.newArrayList(rows).iterator();
  }
}