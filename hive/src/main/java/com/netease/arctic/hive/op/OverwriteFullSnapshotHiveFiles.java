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
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.RefUtil;
import com.netease.arctic.utils.TableFileUtil;
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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class OverwriteFullSnapshotHiveFiles implements OverwriteFiles {
  private final Transaction transaction;
  private final boolean insideTransaction;
  private final UnkeyedHiveTable table;
  private final HMSClientPool hmsClient;
  private final HMSClientPool transactionClient;
  private final OverwriteFiles delegate;
  protected final String db;
  protected final String tableName;

  protected final Table hiveTable;

  private String branch;
  private String fileLocation;
  private final List<DataFile> dataFiles = Lists.newArrayList();

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
      dataFiles.add(file);
    }
    delegate.addFile(file);
    return this;
  }

  @Override
  public OverwriteFiles deleteFile(DataFile file) {
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
    Preconditions.checkArgument(dataFiles.isEmpty(), "branch can only be set before add files");
    delegate.toBranch(branch);
    this.branch = branch;
    return this;
  }

  @Override
  public Snapshot apply() {
    return delegate.apply();
  }

  @Override
  public void commit() {
    // in seconds
    int commitTimestamp = (int) (System.currentTimeMillis() / 1000);
    delegate.commit();
    if (!insideTransaction) {
      transaction.commitTransaction();
    }
    if (branch != null && fileLocation != null) {
      ArrayList<String> values = Lists.newArrayList();
      values.add(getPartitionValue());
      Partition partition = HivePartitionUtil.newPartition(hiveTable, values, fileLocation, dataFiles, commitTimestamp);
      try {
        transactionClient.run(c -> c.addPartitions(Lists.newArrayList(partition)));
      } catch (TException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public String getPartitionValue() {
    Map<String, String> properties = table.properties();
    String branchFormat = PropertyUtil.propertyAsString(properties, TableProperties.AUTO_CREATE_TAG_BRANCH_FORMAT,
        TableProperties.AUTO_CREATE_TAG_BRANCH_FORMAT_DEFAULT);
    String hiveFormat = PropertyUtil.propertyAsString(properties, HiveTableProperties.HIVE_PARTITION_FORMAT,
        HiveTableProperties.HIVE_PARTITION_FORMAT_DEFAULT);
    LocalDate date = RefUtil.getDateOfRef(branch, branchFormat);
    return RefUtil.getDayRefName(date, hiveFormat);
  }
}
