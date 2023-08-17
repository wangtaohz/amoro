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

package com.netease.arctic.hive.op;

import com.netease.arctic.hive.HMSClientPool;
import com.netease.arctic.hive.HiveTableProperties;
import com.netease.arctic.hive.table.UnkeyedHiveTable;
import com.netease.arctic.hive.utils.HivePartitionUtil;
import com.netease.arctic.io.ArcticHadoopFileIO;
import com.netease.arctic.utils.RefUtil;
import com.netease.arctic.utils.TableFileUtil;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.netease.arctic.hive.op.UpdateHiveFiles.DELETE_UNTRACKED_HIVE_FILE;

public class OverwriteFullSnapshotHiveFiles implements OverwriteFiles {
  private static final Logger LOG = LoggerFactory.getLogger(OverwriteFullSnapshotHiveFiles.class);

  private final Transaction transaction;
  private final boolean insideTransaction;
  private final UnkeyedHiveTable table;
  private final HMSClientPool hmsClient;
  private final HMSClientPool transactionClient;
  private final OverwriteFiles delegate;
  protected final String db;
  protected final String tableName;

  protected final Table hiveTable;
  private boolean checkOrphanFiles = false;

  private String branch;
  private String fileLocation;
  private final List<DataFile> addFiles = Lists.newArrayList();
  private final List<DataFile> deleteFiles = Lists.newArrayList();

  public OverwriteFullSnapshotHiveFiles(Transaction transaction, boolean insideTransaction, UnkeyedHiveTable table,
                                        HMSClientPool hmsClient, HMSClientPool transactionClient) {
    this.transaction = transaction;
    this.insideTransaction = insideTransaction;
    this.table = table;
    this.hmsClient = hmsClient;
    this.transactionClient = transactionClient;
    this.db = table.id().getDatabase();
    this.tableName = table.id().getTableName();
    try {
      this.hiveTable = hmsClient.run(c -> c.getTable(db, tableName));
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    this.delegate = transaction.newOverwrite();
  }

  @Override
  public OverwriteFiles overwriteByRowFilter(Expression expr) {
    delegate.overwriteByRowFilter(expr);
    return this;
  }

  @Override
  public OverwriteFiles addFile(DataFile file) {
    if (branch != null) {
      String fileDir = TableFileUtil.getFileDir(file.path().toString());
      if (fileLocation == null) {
        fileLocation = fileDir;
      } else {
        Preconditions.checkArgument(fileLocation.equals(fileDir), "All files must be in the same directory");
      }
      addFiles.add(file);
    }
    delegate.addFile(file);
    return this;
  }

  @Override
  public OverwriteFiles deleteFile(DataFile file) {
    if (branch != null) {
      deleteFiles.add(file);
    }
    delegate.deleteFile(file);
    return this;
  }

  @Override
  public OverwriteFiles validateAddedFilesMatchOverwriteFilter() {
    delegate.validateAddedFilesMatchOverwriteFilter();
    return this;
  }

  @Override
  public OverwriteFiles validateFromSnapshot(long snapshotId) {
    delegate.validateFromSnapshot(snapshotId);
    return this;
  }

  @Override
  public OverwriteFiles caseSensitive(boolean caseSensitive) {
    delegate.caseSensitive(caseSensitive);
    return this;
  }

  @Override
  public OverwriteFiles conflictDetectionFilter(Expression conflictDetectionFilter) {
    delegate.conflictDetectionFilter(conflictDetectionFilter);
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingData() {
    delegate.validateNoConflictingData();
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingDeletes() {
    delegate.validateNoConflictingDeletes();
    return this;
  }

  @Override
  public OverwriteFiles set(String property, String value) {
    if (DELETE_UNTRACKED_HIVE_FILE.equals(property)) {
      this.checkOrphanFiles = Boolean.parseBoolean(value);
    }
    delegate.set(property, value);
    return this;
  }

  @Override
  public OverwriteFiles deleteWith(Consumer<String> deleteFunc) {
    delegate.deleteWith(deleteFunc);
    return this;
  }

  @Override
  public OverwriteFiles stageOnly() {
    delegate.stageOnly();
    return this;
  }

  @Override
  public OverwriteFiles scanManifestsWith(ExecutorService executorService) {
    delegate.scanManifestsWith(executorService);
    return this;
  }

  @Override
  public OverwriteFiles toBranch(String branch) {
    Preconditions.checkArgument(addFiles.isEmpty(), "branch can only be set before add files");
    delegate.toBranch(branch);
    this.branch = branch;
    return this;
  }

  /**
   * check files in the partition, and delete orphan files
   */
  private void checkPartitionedOrphanFilesAndDelete() {
    Set<String> addFilesPathCollect = addFiles.stream()
        .map(dataFile -> dataFile.path().toString()).collect(Collectors.toSet());
    Set<String> deleteFilesPathCollect = deleteFiles.stream()
        .map(deleteFile -> deleteFile.path().toString()).collect(Collectors.toSet());

    try (ArcticHadoopFileIO io = table.io()) {
      io.listPrefix(fileLocation).forEach(f -> {
        if (!addFilesPathCollect.contains(f.location()) &&
            !deleteFilesPathCollect.contains(f.location())) {
          io.deleteFile(f.location());
          LOG.warn("Delete orphan file path: {}", f.location());
        }
      });
    }
  }

  @Override
  public Snapshot apply() {
    return delegate.apply();
  }

  @Override
  public Object updateEvent() {
    return delegate.updateEvent();
  }

  @Override
  public void commit() {
    // in seconds
    int commitTimestamp = (int) (System.currentTimeMillis() / 1000);
    if (branch != null && checkOrphanFiles) {
      checkPartitionedOrphanFilesAndDelete();
    }
    delegate.commit();
    if (!insideTransaction) {
      transaction.commitTransaction();
    }
    if (branch != null && fileLocation != null) {
      ArrayList<String> values = Lists.newArrayList();
      values.add(getPartitionValue());
      Partition partition = HivePartitionUtil.newPartition(hiveTable, values, fileLocation, addFiles, commitTimestamp);
      // if partition exist, alter hive partition
      try {
        hmsClient.run(c -> c.getPartition(db, tableName, partition.getValues()));
        try {
          transactionClient.run(c -> {
            try {
              c.alterPartition(db, tableName, partition, null);
            } catch (InvocationTargetException |
                IllegalAccessException | NoSuchMethodException |
                ClassNotFoundException e) {
              throw new RuntimeException(e);
            }
            return null;
          });
        } catch (TException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      } catch (NoSuchObjectException e) {
        try {
          transactionClient.run(c -> c.addPartition(partition));
        } catch (TException | InterruptedException e1) {
          throw new RuntimeException(e1);
        }
      } catch (TException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public String getPartitionValue() {
    Map<String, String> properties = table.properties();
    String hiveFormat = PropertyUtil.propertyAsString(properties, HiveTableProperties.HIVE_PARTITION_FORMAT,
        HiveTableProperties.HIVE_PARTITION_FORMAT_DEFAULT);
    LocalDate date = RefUtil.getDateOfBranch(branch, properties);
    return RefUtil.getDayRefName(date, hiveFormat);
  }
}
