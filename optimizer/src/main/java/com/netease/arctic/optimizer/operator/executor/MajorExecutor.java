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

package com.netease.arctic.optimizer.operator.executor;

import com.netease.arctic.ams.api.OptimizeType;
import com.netease.arctic.data.DataFileType;
import com.netease.arctic.data.DataTreeNode;
import com.netease.arctic.data.PrimaryKeyedFile;
import com.netease.arctic.hive.io.reader.AdaptHiveGenericArcticDataReader;
import com.netease.arctic.hive.io.writer.AdaptHiveGenericTaskWriterBuilder;
import com.netease.arctic.hive.table.SupportHive;
import com.netease.arctic.hive.utils.HiveTableUtil;
import com.netease.arctic.hive.utils.TableTypeUtil;
import com.netease.arctic.optimizer.OptimizerConfig;
import com.netease.arctic.scan.ArcticFileScanTask;
import com.netease.arctic.scan.BasicArcticFileScanTask;
import com.netease.arctic.scan.KeyedTableScanTask;
import com.netease.arctic.scan.NodeFileScanTask;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.PrimaryKeySpec;
import com.netease.arctic.table.WriteOperationKind;
import com.netease.arctic.utils.TableFileUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.TaskWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MajorExecutor extends AbstractExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(MajorExecutor.class);

  public MajorExecutor(NodeTask nodeTask, ArcticTable table, long startTime, OptimizerConfig config) {
    super(nodeTask, table, startTime, config);
  }

  protected long inputSize() {
    return task.dataFiles().stream().mapToLong(DataFile::fileSizeInBytes).sum();
  }

  @Override
  public OptimizeTaskResult execute() throws Exception {
    Iterable<DataFile> targetFiles;
    LOG.info("Start processing arctic table major optimize task {} of {}: {}", task.getTaskId(),
        task.getTableIdentifier(), task);
    if (TableTypeUtil.isHive(table) && task.isCopyFiles()) {
      if (!task.posDeleteFiles().isEmpty() || !task.deleteFiles().isEmpty()) {
        LOG.info("Task {} of {} enables copy files, but has delete files {} {}, should not copy", task.getTaskId(),
            task.getTableIdentifier(), task.posDeleteFiles().size(), task.deleteFiles().size());
      } else if (task.getCustomHiveSubdirectory() == null) {
        LOG.info("Task {} of {} enables copy files, but custom hive directory is null, should not copy",
            task.getTaskId(),
            task.getTableIdentifier());
      } else {
        LOG.info("Task {} of {} enables copy files, copy {} data files", task.getTaskId(), task.getTableIdentifier(),
            task.dataFiles().size());
        targetFiles = copyFiles(task.dataFiles(), task.getCustomHiveSubdirectory());
        return buildOptimizeResult(targetFiles);
      }
    }

    Map<DataTreeNode, List<DeleteFile>> deleteFileMap = groupDeleteFilesByNode(task.posDeleteFiles());
    List<PrimaryKeyedFile> dataFiles = task.dataFiles();
    dataFiles.addAll(task.deleteFiles());
    targetFiles = table.io().doAs(() -> {
      CloseableIterator<Record> recordIterator =
          openTask(dataFiles, deleteFileMap, table.schema(), task.getSourceNodes());
      return optimizeTable(recordIterator);
    });

    return buildOptimizeResult(targetFiles);
  }

  private Iterable<DataFile> copyFiles(List<PrimaryKeyedFile> dataFiles, String customHiveSubdirectory) {
    int count = 0;
    List<DataFile> targetFiles = new ArrayList<>(dataFiles.size());
    for (PrimaryKeyedFile dataFile : dataFiles) {
      String hiveLocation = HiveTableUtil.newHiveDataLocation(((SupportHive) table).hiveLocation(),
          table.spec(), dataFile.partition(), customHiveSubdirectory);

      String sourcePath = dataFile.path().toString();
      String sourceFileName = TableFileUtils.getFileName(sourcePath);

      String targetPath;
      if (sourceFileName.startsWith(".")) {
        targetPath = hiveLocation + File.separator + sourceFileName;
      } else {
        targetPath = hiveLocation + File.separator + "." + sourceFileName;
      }
      LOG.info("[{}] Start copying file from {} to {}", count++, sourcePath, targetPath);
      long startTime = System.currentTimeMillis();
      table.io().copy(sourcePath, targetPath);
      LOG.info("Successfully copied file {}, cost {} ms", sourceFileName,
          System.currentTimeMillis() - startTime);
      targetFiles.add(DataFiles.builder(table.spec()).copy(dataFile).withPath(targetPath).build());
    }
    return targetFiles;
  }

  @Override
  public void close() {
  }

  private Iterable<DataFile> optimizeTable(CloseableIterator<Record> recordIterator) throws Exception {
    Long transactionId;
    if (table.isKeyedTable()) {
      transactionId = getMaxTransactionId(task.dataFiles());
    } else {
      transactionId = null;
    }
    long targetFileSize = targetFileSize(inputSize());
    TaskWriter<Record> writer = AdaptHiveGenericTaskWriterBuilder.builderFor(table)
        .withTransactionId(transactionId)
        .withTaskId(task.getAttemptId())
        .withCustomHiveSubdirectory(task.getCustomHiveSubdirectory())
        .withTargetFileSize(targetFileSize)
        .buildWriter(task.getOptimizeType() == OptimizeType.Major ?
            WriteOperationKind.MAJOR_OPTIMIZE : WriteOperationKind.FULL_OPTIMIZE);
    long insertCount = 0;
    try {
      while (recordIterator.hasNext()) {
        checkIfTimeout(writer);

        Record baseRecord = recordIterator.next();
        writer.write(baseRecord);
        insertCount++;
        if (insertCount % SAMPLE_DATA_INTERVAL == 1) {
          LOG.info("task {} of {} insert records number {} and data sampling {}",
              task.getTaskId(), task.getTableIdentifier(), insertCount, baseRecord);
        }
      }
    } finally {
      recordIterator.close();
    }

    LOG.info("task {} of {} insert records number {}", task.getTaskId(), task.getTableIdentifier(), insertCount);

    return Arrays.asList(writer.complete().dataFiles());
  }

  private CloseableIterator<Record> openTask(List<PrimaryKeyedFile> dataFiles,
                                             Map<DataTreeNode, List<DeleteFile>> deleteFileMap,
                                             Schema requiredSchema, Set<DataTreeNode> sourceNodes) {
    if (CollectionUtils.isEmpty(dataFiles)) {
      return CloseableIterator.empty();
    }

    PrimaryKeySpec primaryKeySpec = PrimaryKeySpec.noPrimaryKey();
    if (table.isKeyedTable()) {
      KeyedTable keyedTable = table.asKeyedTable();
      primaryKeySpec = keyedTable.primaryKeySpec();
    }

    AdaptHiveGenericArcticDataReader arcticDataReader =
        new AdaptHiveGenericArcticDataReader(table.io(), table.schema(), requiredSchema, primaryKeySpec,
        table.properties().get(TableProperties.DEFAULT_NAME_MAPPING), false,
        IdentityPartitionConverters::convertConstant, sourceNodes, false, structLikeCollections);

    List<ArcticFileScanTask> fileScanTasks = dataFiles.stream()
        .map(file -> {
          if (file.type() == DataFileType.EQ_DELETE_FILE) {
            return new BasicArcticFileScanTask(file, null, table.spec());
          } else {
            return new BasicArcticFileScanTask(file,
                deleteFileMap.get(file.node()), table.spec());
          }
        })
        .collect(Collectors.toList());

    KeyedTableScanTask keyedTableScanTask = new NodeFileScanTask(fileScanTasks);
    LOG.info("start read data : task {} of {}", task.getTaskId(), task.getTableIdentifier());
    return arcticDataReader.readData(keyedTableScanTask);
  }
}
