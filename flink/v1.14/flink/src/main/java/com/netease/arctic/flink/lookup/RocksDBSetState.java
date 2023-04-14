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

import com.ibm.icu.util.ByteArrayWrapper;
import com.netease.arctic.log.Bytes;
import com.netease.arctic.utils.map.RocksDBBackend;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Guava cache structure: key -> list, the elements of this list are rocksdb keys.
 * RocksDB structure: element -> empty.
 */
public class RocksDBSetState extends RocksDBState<List<byte[]>> {

  protected BinaryRowDataSerializerWrapper keySerializer;

  private static final byte[] EMPTY = new byte[0];

  public RocksDBSetState(
      RocksDBBackend rocksDB,
      String columnFamilyName,
      long lruMaximumSize,
      BinaryRowDataSerializerWrapper keySerialization,
      BinaryRowDataSerializerWrapper elementSerialization,
      BinaryRowDataSerializerWrapper valueSerializer,
      int writeRocksDBThreadNum) {
    super(
        rocksDB,
        columnFamilyName,
        lruMaximumSize,
        elementSerialization,
        valueSerializer,
        writeRocksDBThreadNum);
    this.keySerializer = keySerialization;
  }

  /**
   * Retrieve the elements of the key.
   * <p>Fetch the Collection from guava cache,
   * if not present, fetch from rocksDB continuously, via prefix key scanning the rocksDB;
   * if present, just return the result.
   *
   * @return not null, but may be empty.
   */
  public List<byte[]> get(RowData key) throws IOException {
    final byte[] keyBytes = serializeKey(key);
    ByteArrayWrapper keyWrap = wrap(keyBytes);
    List<byte[]> result = guavaCache.getIfPresent(keyWrap);
    if (result == null) {
      try (RocksDBBackend.ValueIterator iterator =
               (RocksDBBackend.ValueIterator) rocksDB.values(columnFamilyName, keyBytes)) {
        result = Lists.newArrayList();
        while (iterator.hasNext()) {
          byte[] targetKeyBytes = iterator.key();
          if (isPrefixKey(targetKeyBytes, keyBytes)) {
            byte[] value = Arrays.copyOfRange(targetKeyBytes, keyBytes.length, targetKeyBytes.length);
            result.add(value);
          }
          iterator.next();
        }
        if (!result.isEmpty()) {
          guavaCache.put(keyWrap, result);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }
    return result;
  }

  private boolean isPrefixKey(byte[] targetKeyBytes, byte[] keyBytes) {
    for (int i = 0; i < keyBytes.length; i++) {
      if (targetKeyBytes[i] != keyBytes[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Merge key and element into guava cache and rocksdb.
   */
  public void merge(RowData joinKey, byte[] uniqueKeyBytes) throws IOException {
    byte[] joinKeyBytes = serializeKey(joinKey);
    byte[] joinKeyAndPrimaryKeyBytes = Bytes.mergeByte(joinKeyBytes, uniqueKeyBytes);
    ByteArrayWrapper keyWrap = wrap(joinKeyBytes);
    if (guavaCache.getIfPresent(keyWrap) != null) {
      guavaCache.invalidate(keyWrap);
    }
    rocksDB.put(columnFamilyName, joinKeyAndPrimaryKeyBytes, EMPTY);
  }

  public void delete(RowData joinKey, byte[] elementBytes) throws IOException {
    final byte[] joinKeyBytes = serializeKey(joinKey);
    ByteArrayWrapper keyWrap = wrap(joinKeyBytes);
    if (guavaCache.getIfPresent(keyWrap) != null) {
      guavaCache.invalidate(keyWrap);
    }
    byte[] joinKeyAndPrimaryKeyBytes = Bytes.mergeByte(joinKeyBytes, elementBytes);
    if (rocksDB.get(columnFamilyName, joinKeyAndPrimaryKeyBytes) != null) {
      rocksDB.delete(columnFamilyName, joinKeyAndPrimaryKeyBytes);
    }
  }

  public void batchWrite(RowData joinKey, byte[] uniqueKeyBytes) throws IOException {
    byte[] joinKeyBytes = serializeKey(joinKey);
    byte[] joinKeyAndPrimaryKeyBytes = Bytes.mergeByte(joinKeyBytes, uniqueKeyBytes);
    RocksDBRecord.OpType opType = convertToOpType(joinKey.getRowKind());
    rocksDBRecordQueue.add(RocksDBRecord.of(opType, joinKeyAndPrimaryKeyBytes, EMPTY));
  }

  @Override
  public void flush() {

  }

  public byte[] serializeKey(RowData key) throws IOException {
    return serializeKey(keySerializer, key);
  }
}
