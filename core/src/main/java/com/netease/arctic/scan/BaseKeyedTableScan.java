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

package com.netease.arctic.scan;

import com.netease.arctic.data.DataFileType;
import com.netease.arctic.data.DataTreeNode;
import com.netease.arctic.table.BaseKeyedTable;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.table.UnkeyedTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base implementation of {@link KeyedTableScan}, including the merge-on-read plan logical
 */
public class BaseKeyedTableScan implements KeyedTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(BaseKeyedTableScan.class);

  private final BaseKeyedTable table;
  List<NodeFileScanTask> splitTasks = new ArrayList<>();
  private final Map<StructLike, List<NodeFileScanTask>> fileScanTasks = new HashMap<>();
  private final int lookBack;
  private final long openFileCost;
  private final long splitSize;
  private Expression expression;

  public BaseKeyedTableScan(BaseKeyedTable table) {
    this.table = table;
    openFileCost = PropertyUtil.propertyAsLong(table.properties(),
        TableProperties.SPLIT_OPEN_FILE_COST, TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
    splitSize = PropertyUtil.propertyAsLong(table.properties(),
        TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT);
    lookBack = PropertyUtil.propertyAsInt(table.properties(),
        TableProperties.SPLIT_LOOKBACK, TableProperties.SPLIT_LOOKBACK_DEFAULT);
  }

  @Override
  public KeyedTableScan filter(Expression expr) {
    if (expression == null) {
      expression = expr;
    } else {
      expression = Expressions.and(expr, expression);
    }
    return this;
  }

  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    // base file
    List<ArcticFileScanTask> changeFileList = new ArrayList<>();
    List<ArcticFileScanTask> baseFileList = new ArrayList<>();
    table.io().doAs(() -> {
      planFiles(table.baseTable()).forEach(
          fileScanTask -> baseFileList.add(new BaseArcticFileScanTask(fileScanTask))
      );
      return null;
    });

    if (table.primaryKeySpec().primaryKeyExisted()) {
      table.io().doAs(() -> {
        planFiles(table.changeTable()).forEach(
            fileScanTask -> changeFileList.add(new BaseArcticFileScanTask(fileScanTask))
        );
        return null;
      });
    }
    LOG.info("mor statistics plan change file size {},base file size {}", changeFileList.size(), baseFileList.size());

    // 1. group files by partition
    Map<StructLike, Collection<ArcticFileScanTask>> partitionedFiles =
        groupFilesByPartition(changeFileList, baseFileList);
    LOG.info("planning table {} need plan partition size {}", table.id(), partitionedFiles.size());
    partitionedFiles.forEach(this::partitionPlan);
    LOG.info("planning table {} partitionPlan end", table.id());
    // 2.split node task (FileScanTask -> FileScanTask List)
    split();
    LOG.info("planning table {} split end", table.id());
    // 3.combine node task (FileScanTask List -> CombinedScanTask)
    CloseableIterable<CombinedScanTask> tasksIterable = combineNode(CloseableIterable.withNoopClose(splitTasks),
        splitSize, lookBack, openFileCost);
    return tasksIterable;
  }

  private CloseableIterable<FileScanTask> planFiles(UnkeyedTable internalTable) {
    TableScan scan = internalTable.newScan();
    if (this.expression != null) {
      scan = scan.filter(this.expression);
    }
    return scan.planFiles();
  }

  private void split() {
    fileScanTasks.forEach((structLike, fileScanTasks1) -> {
      Iterator<NodeFileScanTask> it = fileScanTasks1.iterator();
      while (it.hasNext()) {
        NodeFileScanTask task = it.next();
        if (task.cost() <= splitSize) {
          splitTasks.add(task);
          continue;
        }
        if (task.dataTasks().size() < 2) {
          splitTasks.add(task);
          continue;
        }
        CloseableIterable<NodeFileScanTask> tasksIterable = splitNode(CloseableIterable.withNoopClose(task.dataTasks()),
            task.arcticEquityDeletes(), splitSize, lookBack, openFileCost);
        List<NodeFileScanTask> tasks =
            org.apache.iceberg.relocated.com.google.common.collect.Lists.newArrayList(tasksIterable);
        splitTasks.addAll(tasks);
      }
    });
  }

  public CloseableIterable<NodeFileScanTask> splitNode(
      CloseableIterable<ArcticFileScanTask> splitFiles,
      List<ArcticFileScanTask> deleteFiles,
      long splitSize, int lookback, long openFileCost) {
    Function<ArcticFileScanTask, Long> weightFunc = task -> Math.max(task.file().fileSizeInBytes(), openFileCost);
    return CloseableIterable.transform(
        CloseableIterable.combine(
            new BinPacking.PackingIterable<>(splitFiles, splitSize, lookback, weightFunc, true),
            splitFiles),
        datafiles -> packingTask(datafiles, deleteFiles));
  }

  private NodeFileScanTask packingTask(List<ArcticFileScanTask> datafiles, List<ArcticFileScanTask> deleteFiles) {
    // TODO Optimization: Add files in batch
    return new NodeFileScanTask(Stream.concat(datafiles.stream(), deleteFiles.stream()).collect(Collectors.toList()));
  }

  public CloseableIterable<CombinedScanTask> combineNode(
      CloseableIterable<NodeFileScanTask> splitFiles,
      long splitSize, int lookback, long openFileCost) {
    Function<NodeFileScanTask, Long> weightFunc = file -> Math.max(file.cost(), openFileCost);
    return CloseableIterable.transform(
        CloseableIterable.combine(
            new BinPacking.PackingIterable<>(splitFiles, splitSize, lookback, weightFunc, true),
            splitFiles),
        BaseCombinedScanTask::new);
  }

  /**
   * Construct tree node task according to partition
   * 1. Put all files into the node they originally belonged to
   * 2. Find all data nodes, traverse, and find the delete that intersects them
   */
  private void partitionPlan(StructLike partition, Collection<ArcticFileScanTask> keyedTableTasks) {
    Map<DataTreeNode, NodeFileScanTask> nodeFileScanTaskMap = new HashMap<>();
    // planfiles() cannot guarantee the uniqueness of the file,
    // so Set<path> here is used to remove duplicate files
    Set<String> pathSets = new HashSet<>();
    keyedTableTasks.stream().forEach(task -> {
      if (!pathSets.contains(task.file().path().toString())) {
        pathSets.add(task.file().path().toString());
        DataTreeNode treeNode = DataTreeNode.fromFile(task.file());
        NodeFileScanTask nodeFileScanTask = nodeFileScanTaskMap.getOrDefault(treeNode, new NodeFileScanTask(treeNode));
        nodeFileScanTask.addFile(task);
        nodeFileScanTaskMap.put(treeNode, nodeFileScanTask);
      }
    });

    nodeFileScanTaskMap.forEach((treeNode, nodeFileScanTask) -> {
      if (!nodeFileScanTask.isDataNode()) {
        return;
      }

      nodeFileScanTaskMap.forEach((treeNode1, nodeFileScanTask1) -> {
        if (!treeNode1.equals(treeNode) && (treeNode1.isSonOf(treeNode) || treeNode.isSonOf(treeNode1))) {
          List<ArcticFileScanTask> deletes = nodeFileScanTask1.arcticEquityDeletes().stream()
              .filter(file -> file.file().mask() == treeNode1.mask() && file.file().index() == treeNode1.index())
              .collect(Collectors.toList());

          nodeFileScanTask.addTasks(deletes);
        }
      });
    });

    List<NodeFileScanTask> fileScanTaskList = new ArrayList<>();
    nodeFileScanTaskMap.forEach((treeNode, nodeFileScanTask) -> {
      if (!nodeFileScanTask.isDataNode()) {
        return;
      }
      fileScanTaskList.add(nodeFileScanTask);
    });

    fileScanTasks.put(partition, fileScanTaskList);
  }

  public Map<StructLike, Collection<ArcticFileScanTask>> groupFilesByPartition(List<ArcticFileScanTask> changeTasks,
      List<ArcticFileScanTask> baseTasks) {
    ListMultimap<StructLike, ArcticFileScanTask> filesGroupedByPartition
        = Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
    StructLikeMap<Long> partitionMaxTxId = table.baseTable().partitionMaxTransactionId();

    // filter change files according to max transaction id
    changeTasks.forEach(task -> {
      StructLike structLike = task.file().partition();
      Long txId = null;
      if (structLike.size() == 0) {
        txId = partitionMaxTxId.get(null);
      } else {
        txId = partitionMaxTxId.get(task.file().partition());
      }
      // Long transactionId = partitionTransactionMap.get(FileUtil.getPartition(task.file()));
      txId = txId == null ? -1 : txId;
      if (task.file().transactionId() > txId) {
        filesGroupedByPartition.put(task.file().partition(), task);
      }
    });

    baseTasks.forEach(task -> {
      filesGroupedByPartition.put(task.file().partition(), task);
    });
    return filesGroupedByPartition.asMap();
  }
}