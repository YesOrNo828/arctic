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

package com.netease.arctic.data;

import org.apache.iceberg.DataFile;

/**
 * Files in {@link com.netease.arctic.table.KeyedTable}.
 */
public interface PrimaryKeyedFile extends DataFile {

  /**
   * Returns the type of file
   */
  DataFileType type();

  //TODO change to DataTreeNode
  long mask();

  long index();

  /**
   * Returns the id of transaction which file added into table
   */
  Long transactionId();

  /**
   * Returns the minimum {@link ChangedLsn} within file
   */
  ChangedLsn minLsn();

  /**
   * Return the maximum {@link ChangedLsn} within file
   */
  ChangedLsn maxLsn();

  /**
   * File information summary for logging
   */
  default String fileInfo() {
    return new StringBuilder().append(path()).append("[").append(fileSizeInBytes()).append("]").toString();
  }
}
