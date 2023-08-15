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

package com.netease.arctic.hive.utils;

import com.netease.arctic.hive.HMSClientPool;
import com.netease.arctic.hive.HiveTableProperties;
import com.netease.arctic.hive.catalog.ArcticHiveCatalog;
import com.netease.arctic.hive.table.SupportHive;
import com.netease.arctic.hive.table.UnkeyedHiveTable;
import com.netease.arctic.io.ArcticHadoopFileIO;
import com.netease.arctic.op.UpdatePartitionProperties;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.PrimaryKeySpec;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.utils.TablePropertyUtil;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class UpgradeHiveTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeHiveTableUtil.class);

  private static final long DEFAULT_TXID = 0L;

  /**
   * Upgrade a hive table to an Arctic table.
   *
   * @param arcticHiveCatalog A arctic catalog adapt hive
   * @param tableIdentifier   A table identifier
   * @param pkList            The name of the columns that needs to be set as the primary key
   * @param properties        Properties to be added to the target table
   */
  public static void upgradeHiveTable(
      ArcticHiveCatalog arcticHiveCatalog, TableIdentifier tableIdentifier,
      List<String> pkList, Map<String, String> properties) throws Exception {
    if (!formatCheck(arcticHiveCatalog.getHMSClient(), tableIdentifier)) {
      throw new IllegalArgumentException("Only support storage format is parquet");
    }

    String baseHivePartitionProjectionMode = CompatibleHivePropertyUtil.propertyAsString(properties,
        HiveTableProperties.BASE_HIVE_PARTITION_PROJECTION,
        HiveTableProperties.BASE_HIVE_PARTITION_PROJECTION_MODE_DEFAULT);

    switch (baseHivePartitionProjectionMode) {
      case HiveTableProperties.BASE_HIVE_PARTITION_PROJECTION_MODE_PARTITION:
        upgradeToPartitionHiveTable(arcticHiveCatalog, tableIdentifier, pkList, properties);
        break;
      case HiveTableProperties.BASE_HIVE_PARTITION_PROJECTION_MODE_TAG:
        upgradeToTagHiveTable(arcticHiveCatalog, tableIdentifier, pkList, properties);
        break;
      default:
        throw new IllegalStateException("Unsupported hive table projection mode " + baseHivePartitionProjectionMode);
    }
  }

  private static void upgradeToPartitionHiveTable(ArcticHiveCatalog arcticHiveCatalog, TableIdentifier tableIdentifier,
                                                  List<String> pkList, Map<String, String> properties)
      throws Exception {
    boolean upgradeHive = false;
    try {
      Table hiveTable = HiveTableUtil.loadHmsTable(arcticHiveCatalog.getHMSClient(), tableIdentifier);

      Schema schema = HiveSchemaUtil.convertHiveSchemaToIcebergSchema(hiveTable, pkList);

      List<FieldSchema> partitionKeys = hiveTable.getPartitionKeys();

      PartitionSpec.Builder partitionBuilder = PartitionSpec.builderFor(schema);
      partitionKeys.stream().forEach(p -> partitionBuilder.identity(p.getName()));

      PrimaryKeySpec.Builder primaryKeyBuilder = PrimaryKeySpec.builderFor(schema);
      pkList.stream().forEach(p -> primaryKeyBuilder.addColumn(p));

      ArcticTable arcticTable = arcticHiveCatalog.newTableBuilder(tableIdentifier, schema)
          .withProperties(properties)
          .withPartitionSpec(partitionBuilder.build())
          .withPrimaryKeySpec(primaryKeyBuilder.build())
          .withProperty(HiveTableProperties.ALLOW_HIVE_TABLE_EXISTED, "true")
          .create();
      upgradeHive = true;
      UpgradeHiveTableUtil.hiveDataMigration((SupportHive) arcticTable, arcticHiveCatalog);
    } catch (Throwable t) {
      if (upgradeHive) {
        arcticHiveCatalog.dropTable(tableIdentifier, false);
      }
      throw t;
    }
  }

  private static void upgradeToTagHiveTable(ArcticHiveCatalog arcticHiveCatalog, TableIdentifier tableIdentifier,
                                            List<String> pkList, Map<String, String> properties) throws Exception {
    if (!partitionFormatCheck(arcticHiveCatalog.getHMSClient(), tableIdentifier, properties)) {
      throw new IllegalArgumentException("Hive Table " + tableIdentifier + " format is not supported, " +
          "please check if your Hive Table partition key is greater than 1, " +
          "and if the Hive Table partition name meets the requirements " +
          CompatibleHivePropertyUtil.propertyAsString(properties, HiveTableProperties.HIVE_PARTITION_FORMAT,
              HiveTableProperties.HIVE_PARTITION_FORMAT_DEFAULT) + ".");
    }

    if (pkList == null || pkList.isEmpty()) {
      throw new IllegalArgumentException("Please set the primary key of the table you want to add tag " +
          tableIdentifier + ".");
    }

    boolean upgradeHive = false;

    try {
      Table hiveTable = HiveTableUtil.loadHmsTable(arcticHiveCatalog.getHMSClient(), tableIdentifier);
      List<FieldSchema> hiveSchema = hiveTable.getSd().getCols();
      hiveSchema.removeAll(hiveTable.getPartitionKeys());
      Set<String> pkSet = new HashSet<>(pkList);
      Schema schema = org.apache.iceberg.hive.HiveSchemaUtil.convert(hiveSchema, true);
      List<Types.NestedField> columnsWithPk = new ArrayList<>();
      schema.columns().forEach(nestedField -> {
        if (pkSet.contains(nestedField.name())) {
          columnsWithPk.add(nestedField.asRequired());
        } else {
          columnsWithPk.add(nestedField);
        }
      });
      schema = new Schema(columnsWithPk);

      PrimaryKeySpec.Builder primaryKeyBuilder = PrimaryKeySpec.builderFor(schema);
      pkList.stream().forEach(p -> primaryKeyBuilder.addColumn(p));

      ArcticTable arcticTable = arcticHiveCatalog.newTableBuilder(tableIdentifier, schema)
          .withProperties(properties)
          .withPrimaryKeySpec(primaryKeyBuilder.build())
          .withProperty(HiveTableProperties.ALLOW_HIVE_TABLE_EXISTED, "true")
          .create();
      upgradeHive = true;
      UpgradeHiveTableUtil.hiveDataMigrationWithTag((SupportHive) arcticTable, arcticHiveCatalog);
    } catch (Throwable t) {
      if (upgradeHive) {
        arcticHiveCatalog.dropTable(tableIdentifier, false);
      }
      throw t;
    }
  }

  private static void hiveDataMigration(SupportHive arcticTable, ArcticHiveCatalog arcticHiveCatalog)
      throws Exception {
    Table hiveTable = HiveTableUtil.loadHmsTable(arcticHiveCatalog.getHMSClient(), arcticTable.id());
    String hiveDataLocation = HiveTableUtil.hiveRootLocation(hiveTable.getSd().getLocation());
    ArcticHadoopFileIO io = arcticTable.io();
    io.makeDirectories(hiveDataLocation);
    String newPath;
    if (hiveTable.getPartitionKeys().isEmpty()) {
      newPath = hiveDataLocation + "/" + System.currentTimeMillis() + "_" + UUID.randomUUID();
      io.makeDirectories(newPath);
      io.listDirectory(hiveTable.getSd().getLocation()).forEach(p -> {
        if (!p.isDirectory()) {
          io.asFileSystemIO().rename(p.location(), newPath);
        }
      });

      try {
        HiveTableUtil.alterTableLocation(arcticHiveCatalog.getHMSClient(), arcticTable.id(), newPath);
        LOG.info("Table {} alter hive table location {}", arcticTable.name(), hiveDataLocation);
      } catch (IOException e) {
        LOG.warn("Table {} alter hive table location failed", arcticTable.name(), e);
        throw new RuntimeException(e);
      }
    } else {
      List<String> partitions =
          HivePartitionUtil.getHivePartitionNames(arcticHiveCatalog.getHMSClient(), arcticTable.id());
      List<String> partitionLocations =
          HivePartitionUtil.getHivePartitionLocations(arcticHiveCatalog.getHMSClient(), arcticTable.id());
      for (int i = 0; i < partitionLocations.size(); i++) {
        String partition = partitions.get(i);
        String oldLocation = partitionLocations.get(i);
        String newLocation = hiveDataLocation + "/" + partition + "/" + HiveTableUtil.newHiveSubdirectory(DEFAULT_TXID);
        io.makeDirectories(newLocation);

        io.listDirectory(oldLocation).forEach(p -> {
          if (!p.isDirectory()) {
            io.asFileSystemIO().rename(p.location(), newLocation);
          }
        });
        HivePartitionUtil.alterPartition(arcticHiveCatalog.getHMSClient(), arcticTable.id(), partition, newLocation);
      }
    }
    HiveMetaSynchronizer.syncHiveDataToArctic(arcticTable, arcticHiveCatalog.getHMSClient());
    hiveTable = HiveTableUtil.loadHmsTable(arcticHiveCatalog.getHMSClient(), arcticTable.id());
    fillPartitionProperties(arcticTable, arcticHiveCatalog, hiveTable);
  }

  private static void hiveDataMigrationWithTag(SupportHive arcticTable, ArcticHiveCatalog arcticHiveCatalog) {
    HiveMetaSynchronizer.syncHiveDataToArcticWithTag(arcticTable, arcticHiveCatalog.getHMSClient());
  }

  /**
   * Check whether Arctic supports the hive table storage formats.
   *
   * @param hiveClient      Hive client from ArcticHiveCatalog
   * @param tableIdentifier A table identifier
   * @return Support or not
   */
  private static boolean formatCheck(HMSClientPool hiveClient, TableIdentifier tableIdentifier) throws IOException {
    AtomicBoolean isSupport = new AtomicBoolean(false);
    try {
      hiveClient.run(client -> {
        Table hiveTable = HiveTableUtil.loadHmsTable(hiveClient, tableIdentifier);
        StorageDescriptor storageDescriptor = hiveTable.getSd();
        SerDeInfo serDeInfo = storageDescriptor.getSerdeInfo();
        switch (storageDescriptor.getInputFormat()) {
          case HiveTableProperties.PARQUET_INPUT_FORMAT:
            if (storageDescriptor.getOutputFormat().equals(HiveTableProperties.PARQUET_OUTPUT_FORMAT) &&
                serDeInfo.getSerializationLib().equals(HiveTableProperties.PARQUET_ROW_FORMAT_SERDE)) {
              isSupport.set(true);
            } else {
              throw new IllegalStateException("Please check your hive table storage format is right");
            }
            break;
          default:
            isSupport.set(false);
            break;
        }
        return null;
      });
    } catch (Exception e) {
      throw new IOException(e);
    }
    return isSupport.get();
  }

  private static boolean partitionFormatCheck(HMSClientPool hiveClient, TableIdentifier tableIdentifier,
                                              Map<String, String> properties) throws TException, InterruptedException {
    String dateFormat =  CompatibleHivePropertyUtil.propertyAsString(properties,
        HiveTableProperties.HIVE_PARTITION_FORMAT,
        HiveTableProperties.HIVE_PARTITION_FORMAT_DEFAULT);
    AtomicBoolean isSupport = new AtomicBoolean(true);

    hiveClient.run(client -> {
      Table hiveTable = HiveTableUtil.loadHmsTable(hiveClient, tableIdentifier);

      if (!hiveTable.isSetPartitionKeys() || hiveTable.getPartitionKeys().size() > 1) {
        isSupport.set(false);
        return null;
      }
      List<String> partitionNames = client
          .listPartitions(tableIdentifier.getDatabase(), tableIdentifier.getTableName(), (short) -1)
          .stream()
          .map(p -> p.getValues().get(0).substring(
              p.getValues().get(0).indexOf("=") + 1)).collect(Collectors.toList());
      for (String partitionName : partitionNames) {
        if (!isValidDate(partitionName, dateFormat)) {
          isSupport.set(false);
          break;
        }
      }
      return null;
    });
    return isSupport.get();
  }

  @VisibleForTesting
  static void fillPartitionProperties(ArcticTable table, ArcticHiveCatalog arcticHiveCatalog, Table hiveTable) {
    UnkeyedHiveTable baseTable;
    if (table.isKeyedTable()) {
      baseTable = (UnkeyedHiveTable) table.asKeyedTable().baseTable();
    } else {
      baseTable = (UnkeyedHiveTable) table.asUnkeyedTable();
    }
    UpdatePartitionProperties updatePartitionProperties =
        baseTable.updatePartitionProperties(null);
    if (table.spec().isUnpartitioned()) {
      if (hasPartitionProperties(baseTable, false, null)) {
        return;
      }
      updatePartitionProperties.set(
          TablePropertyUtil.EMPTY_STRUCT,
          HiveTableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION, baseTable.hiveLocation());
      updatePartitionProperties.set(
          TablePropertyUtil.EMPTY_STRUCT,
          HiveTableProperties.PARTITION_PROPERTIES_KEY_TRANSIENT_TIME,
          hiveTable.getParameters().get("transient_lastDdlTime"));
    } else {
      List<Partition> partitions =
          HivePartitionUtil.getHiveAllPartitions(arcticHiveCatalog.getHMSClient(), table.id());
      partitions.forEach(partition -> {
        StructLike partitionData = HivePartitionUtil.buildPartitionData(partition.getValues(), table.spec());
        if (hasPartitionProperties(baseTable, true, partitionData)) {
          return;
        }
        updatePartitionProperties.set(
            partitionData,
            HiveTableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION,
            partition.getSd().getLocation());
        updatePartitionProperties.set(
            partitionData,
            HiveTableProperties.PARTITION_PROPERTIES_KEY_TRANSIENT_TIME,
            partition.getParameters().get("transient_lastDdlTime"));
      });
    }
    updatePartitionProperties.commit();
  }

  private static boolean hasPartitionProperties(
      UnkeyedHiveTable baseTable,
      boolean isPartitioned,
      StructLike partitionData) {
    Map<String, String> partitionProperties = isPartitioned ? baseTable.partitionProperty().get(partitionData) :
        baseTable.partitionProperty().get(TablePropertyUtil.EMPTY_STRUCT);
    return partitionProperties != null &&
        partitionProperties.containsKey(HiveTableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION) &&
        partitionProperties.containsKey(HiveTableProperties.PARTITION_PROPERTIES_KEY_TRANSIENT_TIME);
  }

  private static boolean isValidDate(String dateString, String dateFormat) {
    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    sdf.setLenient(false);

    try {
      Date date = sdf.parse(dateString);

      if (date.getHours() != 0 || date.getMinutes() != 0 || date.getSeconds() != 0) {
        return false;
      }

      String pattern = sdf.toPattern();
      if (!pattern.contains("d") && !pattern.contains("dd")) {
        return false;
      }

      int yearIndex = pattern.indexOf('y');
      int monthIndex = pattern.indexOf('M');

      if (yearIndex == -1 || (monthIndex == -1 && monthIndex != yearIndex + 1)) {
        return false;
      }

      return true;
    } catch (ParseException e) {
      return false;
    }
  }
}
