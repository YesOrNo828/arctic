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

import com.netease.arctic.table.TableMetaStore;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Implementation of {@link ArcticFileIO} for hadoop file system with authentication.
 */
public class ArcticHadoopFileIO extends HadoopFileIO implements ArcticFileIO {
  private static final Logger LOG = LoggerFactory.getLogger(ArcticHadoopFileIO.class);
  private final TableMetaStore tableMetaStore;

  public ArcticHadoopFileIO(TableMetaStore tableMetaStore) {
    super(tableMetaStore.getConfiguration());
    this.tableMetaStore = tableMetaStore;
  }

  @Override
  public InputFile newInputFile(String path) {
    return tableMetaStore.doAs(() -> super.newInputFile(path));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return tableMetaStore.doAs(() -> super.newOutputFile(path));
  }

  @Override
  public void deleteFile(String path) {
    tableMetaStore.doAs(() -> {
      Path toDelete = new Path(path);
      FileSystem fs = getFs(toDelete);

      try {
        fs.delete(toDelete, false);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to delete file: " + new Object[]{path}, e);
      }
      return null;
    });
  }

  @Override
  public boolean deleteFileWithResult(String path) {
    return tableMetaStore.doAs(() -> {
      Path toDelete = new Path(path);
      FileSystem fs = getFs(toDelete);
      boolean result;
      try {
        result = fs.delete(toDelete, false /* not recursive */);
      } catch (IOException e) {
        result = false;
      }
      return result;
    });
  }

  @Override
  public boolean deleteFileWithResult(String path, boolean recursive) {
    return tableMetaStore.doAs(() -> {
      Path toDelete = new Path(path);
      FileSystem fs = getFs(toDelete);
      boolean result;
      try {
        result = fs.delete(toDelete, recursive);
      } catch (IOException e) {
        result = false;
      }
      return result;
    });
  }

  @Override
  public List<FileStatus> list(String location) {
    return tableMetaStore.doAs(() -> {
      Path path = new Path(location);
      FileSystem fs = getFs(path);
      try {
        FileStatus[] fileStatuses = fs.listStatus(path);
        return Lists.newArrayList(fileStatuses);
      } catch (IOException e) {
        throw new UncheckedIOException("Fail to list files in " + location, e);
      }
    });
  }


  @VisibleForTesting
  public List<FileStatus> list(String location, Callable<List<FileStatus>> callable) {
    return tableMetaStore.doAs(() -> {
      callable.call();
      Path path = new Path(location);
      FileSystem fs = getFs(path);
      try {
        FileStatus[] fileStatuses = fs.listStatus(path);
        return Lists.newArrayList(fileStatuses);
      } catch (IOException e) {
        throw new UncheckedIOException("Fail to list files in " + location, e);
      }
    });
  }

  @VisibleForTesting
  public List<FileStatus> listWithoutDoAs(String location) {
    Path path = new Path(location);
    FileSystem fs = getFs(path);
    try {
      FileStatus[] fileStatuses = fs.listStatus(path);
      return Lists.newArrayList(fileStatuses);
    } catch (IOException e) {
      throw new UncheckedIOException("Fail to list files in " + location, e);
    }
  }

  @Override
  public boolean isDirectory(String location) {
    return tableMetaStore.doAs(() -> {
      Path path = new Path(location);
      FileSystem fs = getFs(path);

      try {
        return fs.isDirectory(path);
      } catch (IOException e) {
        throw new UncheckedIOException("Fail to check file directory for %s", e);
      }
    });
  }

  @Override
  public boolean isEmptyDirectory(String location) {
    Preconditions.checkArgument(isDirectory(location), "the target location is not directory");
    return tableMetaStore.doAs(() -> {
      Path path = new Path(location);
      FileSystem fs = getFs(path);
      try {
        RemoteIterator<LocatedFileStatus> fileStatuses = fs.listFiles(path, true);
        return !fileStatuses.hasNext();
      } catch (IOException e) {
        throw new UncheckedIOException("Fail to list files in " + location, e);
      }
    });
  }

  @Override
  public <T> T doAs(Callable<T> callable) {
    return tableMetaStore.doAs(callable);
  }

  @Override
  public boolean exists(String path) {
    return tableMetaStore.doAs(() -> {
      Path filePath = new Path(path);
      FileSystem fs = getFs(filePath);
      try {
        return fs.exists(filePath);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to check file exist for " + path, e);
      }
    });
  }

  @Override
  public boolean rename(String src, String dts) {
    return tableMetaStore.doAs(() -> {
      Path srcPath = new Path(src);
      Path dtsPath = new Path(dts);
      FileSystem fs = getFs(srcPath);
      try {
        return fs.rename(srcPath, dtsPath);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to rename: from " + src + " to " + dts, e);
      }
    });
  }

  @Override
  public boolean mkdirs(String src) {
    return tableMetaStore.doAs(() -> {
      Path path = new Path(src);
      FileSystem fs = getFs(path);
      try {
        return fs.mkdirs(path);
      } catch (IOException e) {
        LOG.error("Failed to mkdirs: path {}, exception {}", path, e);
        throw new UncheckedIOException("Failed to mkdirs: path " + src, e);
      }
    });
  }

  private FileSystem getFs(Path path) {
    return Util.getFs(path, conf());
  }
}
