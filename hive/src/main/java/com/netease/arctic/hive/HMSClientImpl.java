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

package com.netease.arctic.hive;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.common.DynMethods;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HMSClientImpl implements HMSClient {
  private static final Logger LOG = LoggerFactory.getLogger(HMSClientImpl.class);

  private HiveMetaStoreClient client;


  public HMSClientImpl(HiveMetaStoreClient client) {
    this.client = client;
  }


  public HiveMetaStoreClient getClient() {
    return this.client;
  }

  @Override
  public void close() {
    getClient().close();
  }

  @Override
  public void reconnect() throws MetaException {
    getClient().reconnect();
  }

  @Override
  public List<String> getAllDatabases() throws TException {
    return getClient().getAllDatabases();
  }

  @Override
  public Partition getPartition(String dbName, String tblName, List<String> partVals) throws TException {
    return getClient().getPartition(dbName, tblName, partVals);
  }

  @Override
  public Partition getPartition(String dbName, String tblName, String name) throws TException {
    return getClient().getPartition(dbName, tblName, name);
  }

  @Override
  public Table getTable(String dbName, String tableName) throws TException {
    return getClient().getTable(dbName, tableName);
  }

  @Override
  public void alterTable(String defaultDatabaseName, String tblName, Table table) throws TException {
    getClient().alter_table(defaultDatabaseName, tblName, table);
  }

  @Override
  public List<Partition> listPartitions(String dbName, String tblName, short maxParts) throws TException {
    return getClient().listPartitions(dbName, tblName, maxParts);
  }

  @Override
  public List<Partition> listPartitions(String dbName, String tblName,
                                        List<String> partVals, short maxParts) throws TException {
    return getClient().listPartitions(dbName, tblName, partVals, maxParts);
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName, short maxParts) throws TException {
    return getClient().listPartitionNames(dbName, tblName, maxParts);
  }

  @Override
  public void createDatabase(Database db) throws TException {
    getClient().createDatabase(db);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData,
                           boolean ignoreUnknownDb, boolean cascade) throws TException {
    getClient().dropDatabase(name, deleteData, ignoreUnknownDb, cascade);
  }

  @Override
  public void dropTable(String dbname, String tableName,
                        boolean deleteData, boolean ignoreUnknownTab) throws TException {
    getClient().dropTable(dbname, tableName, deleteData, ignoreUnknownTab);
  }

  @Override
  public void createTable(Table tbl) throws TException {
    getClient().createTable(tbl);
  }

  @Override
  public Database getDatabase(String databaseName) throws TException {
    return getClient().getDatabase(databaseName);
  }

  @Override
  public Partition addPartition(Partition partition) throws TException {
    Partition result = getClient().add_partition(partition);
    LOG.info("add hive partition success {}.{} {}", partition.getDbName(), partition.getTableName(),
        partition.getValues());
    return result;
  }

  @Override
  public boolean dropPartition(String dbName, String tblName,
                               List<String> partVals, PartitionDropOptions options) throws TException {
    boolean result = getClient().dropPartition(dbName, tblName, partVals, options);
    LOG.info("drop hive partition success {}.{} {}", dbName, tblName, partVals);
    return result;
  }

  @Override
  public int addPartitions(List<Partition> partitions) throws TException {
    int result = getClient().add_partitions(partitions);
    LOG.info("add hive partitions success {}", partitions);
    return result;
  }


  @Override
  public List<String> getAllTables(String dbName) throws TException {
    return getClient().getAllTables(dbName);
  }

  @Override
  public void alterPartitions(String dbName, String tblName,
                              List<Partition> newParts, EnvironmentContext environmentContext) {
    DynMethods.UnboundMethod alterPartitions = DynMethods.builder("alter_partitions")
        .impl(HiveMetaStoreClient.class, String.class, String.class, List.class, EnvironmentContext.class)
        .impl(HiveMetaStoreClient.class, String.class, String.class, List.class)
        .build();
    alterPartitions.invoke(getClient(), dbName, tblName, newParts, environmentContext);
    LOG.info("{}.{} alter hive partitions success {}", dbName, tblName, newParts);
  }

  @Override
  public void alterPartition(String dbName, String tblName,
                             Partition newPart, EnvironmentContext environmentContext) {
    DynMethods.UnboundMethod alterPartition = DynMethods.builder("alter_partition")
        .impl(HiveMetaStoreClient.class, String.class, String.class, Partition.class, EnvironmentContext.class)
        .impl(HiveMetaStoreClient.class, String.class, String.class, Partition.class)
        .build();
    alterPartition.invoke(getClient(), dbName, tblName, newPart, environmentContext);
    LOG.info("{}.{} alter hive partition success {}", dbName, tblName, newPart);
  }
}
