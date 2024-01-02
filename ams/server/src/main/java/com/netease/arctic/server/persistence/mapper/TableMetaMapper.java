package com.netease.arctic.server.persistence.mapper;

import com.netease.arctic.ams.api.ServerTableIdentifier;
import com.netease.arctic.ams.api.config.TableConfiguration;
import com.netease.arctic.ams.api.process.PendingInput;
import com.netease.arctic.server.persistence.TableRuntimePersistency;
import com.netease.arctic.server.persistence.converter.JsonObjectConverter;
import com.netease.arctic.server.persistence.converter.Long2TsConverter;
import com.netease.arctic.server.persistence.converter.Map2StringConverter;
import com.netease.arctic.server.persistence.converter.MapLong2StringConverter;
import com.netease.arctic.server.table.DefaultTableRuntime;
import com.netease.arctic.server.table.TableMetadata;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

public interface TableMetaMapper {

  @Insert("INSERT INTO database_metadata(catalog_name, db_name) VALUES( #{catalogName}, #{dbName})")
  void insertDatabase(@Param("catalogName") String catalogName, @Param("dbName") String dbName);

  @Select("SELECT db_name FROM database_metadata WHERE catalog_name = #{catalogName}")
  List<String> selectDatabases(@Param("catalogName") String catalogName);

  @Select(
      "SELECT db_name FROM database_metadata WHERE catalog_name = #{catalogName} AND db_name=#{dbName}")
  String selectDatabase(@Param("catalogName") String catalogName, @Param("dbName") String dbName);

  @Delete(
      "DELETE FROM database_metadata WHERE catalog_name = #{catalogName} AND db_name = #{dbName}"
          + " AND table_count = 0")
  Integer dropDb(@Param("catalogName") String catalogName, @Param("dbName") String dbName);

  @Update(
      "UPDATE database_metadata SET table_count = table_count + #{tableCount} WHERE db_name = #{databaseName}")
  Integer incTableCount(
      @Param("tableCount") Integer tableCount, @Param("databaseName") String databaseName);

  @Update(
      "UPDATE database_metadata SET table_count = table_count - #{tableCount} WHERE db_name = #{databaseName}")
  Integer decTableCount(
      @Param("tableCount") Integer tableCount, @Param("databaseName") String databaseName);

  @Select("SELECT table_count FROM database_metadata WHERE db_name = #{databaseName}")
  Integer selectTableCount(@Param("databaseName") String databaseName);

  @Select(
      "SELECT m.table_id, i.table_name, i.db_name, i.catalog_name, i.format, primary_key, "
          + "table_location, base_location, change_location, meta_store_site, hdfs_site, core_site, "
          + "auth_method, hadoop_username, krb_keytab, krb_conf, krb_principal, properties, meta_version "
          + "FROM table_metadata m INNER JOIN table_identifier i ON m.table_id = i.table_id ")
  @Results({
    @Result(property = "tableIdentifier.id", column = "table_id"),
    @Result(property = "tableIdentifier.tableName", column = "table_name"),
    @Result(property = "tableIdentifier.database", column = "db_name"),
    @Result(property = "tableIdentifier.catalog", column = "catalog_name"),
    @Result(property = "tableIdentifier.format", column = "format"),
    @Result(property = "primaryKey", column = "primary_key"),
    @Result(property = "tableLocation", column = "table_location"),
    @Result(property = "baseLocation", column = "base_location"),
    @Result(property = "changeLocation", column = "change_location"),
    @Result(property = "metaStoreSite", column = "meta_store_site"),
    @Result(property = "hdfsSite", column = "hdfs_site"),
    @Result(property = "coreSite", column = "core_site"),
    @Result(property = "authMethod", column = "auth_method"),
    @Result(property = "hadoopUsername", column = "hadoop_username"),
    @Result(property = "krbKeytab", column = "krb_keytab"),
    @Result(property = "krbConf", column = "krb_conf"),
    @Result(property = "krbPrincipal", column = "krb_principal"),
    @Result(
        property = "properties",
        column = "properties",
        typeHandler = Map2StringConverter.class),
    @Result(property = "metaVersion", column = "meta_version")
  })
  List<TableMetadata> selectTableMetas();

  @Select(
      "SELECT table_identifier.table_id as table_id, table_identifier.catalog_name as catalog_name, "
          + "table_identifier.db_name as db_name, table_identifier.table_name as table_name, table_identifier.format, "
          + "primary_key, "
          + "table_location, base_location, change_location, meta_store_site, hdfs_site, core_site, "
          + "auth_method, hadoop_username, krb_keytab, krb_conf, krb_principal, properties, meta_version "
          + "FROM table_metadata INNER JOIN table_identifier ON table_metadata.table_id=table_identifier.table_id "
          + "WHERE "
          + "table_identifier.catalog_name=#{catalogName} AND table_identifier.db_name=#{database}")
  @Results({
    @Result(property = "tableIdentifier.id", column = "table_id"),
    @Result(property = "tableIdentifier.catalog", column = "catalog_name"),
    @Result(property = "tableIdentifier.database", column = "db_name"),
    @Result(property = "tableIdentifier.tableName", column = "table_name"),
    @Result(property = "tableIdentifier.format", column = "format"),
    @Result(property = "primaryKey", column = "primary_key"),
    @Result(property = "tableLocation", column = "table_location"),
    @Result(property = "baseLocation", column = "base_location"),
    @Result(property = "changeLocation", column = "change_location"),
    @Result(property = "metaStoreSite", column = "meta_store_site"),
    @Result(property = "hdfsSite", column = "hdfs_site"),
    @Result(property = "coreSite", column = "core_site"),
    @Result(property = "authMethod", column = "auth_method"),
    @Result(property = "hadoopUsername", column = "hadoop_username"),
    @Result(property = "krbKeytab", column = "krb_keytab"),
    @Result(property = "krbConf", column = "krb_conf"),
    @Result(property = "krbPrincipal", column = "krb_principal"),
    @Result(
        property = "properties",
        column = "properties",
        typeHandler = Map2StringConverter.class),
    @Result(property = "metaVersion", column = "meta_version")
  })
  List<TableMetadata> selectTableMetasByDb(
      @Param("catalogName") String catalogName, @Param("database") String database);

  @Insert(
      "INSERT INTO table_metadata(table_id, table_name, db_name, catalog_name, primary_key,"
          + " table_location, base_location, change_location, meta_store_site, hdfs_site, core_site,"
          + " auth_method, hadoop_username, krb_keytab, krb_conf, krb_principal, properties, meta_version)"
          + " VALUES("
          + " #{tableMeta.tableIdentifier.id},"
          + " #{tableMeta.tableIdentifier.tableName},"
          + " #{tableMeta.tableIdentifier.database},"
          + " #{tableMeta.tableIdentifier.catalog},"
          + " #{tableMeta.primaryKey, jdbcType=VARCHAR},"
          + " #{tableMeta.tableLocation, jdbcType=VARCHAR},"
          + " #{tableMeta.baseLocation, jdbcType=VARCHAR},"
          + " #{tableMeta.changeLocation, jdbcType=VARCHAR},"
          + " #{tableMeta.metaStoreSite, jdbcType=VARCHAR}, "
          + " #{tableMeta.hdfsSite, jdbcType=VARCHAR},"
          + " #{tableMeta.coreSite, jdbcType=VARCHAR},"
          + " #{tableMeta.authMethod, jdbcType=VARCHAR},"
          + " #{tableMeta.hadoopUsername, jdbcType=VARCHAR},"
          + " #{tableMeta.krbKeytab, jdbcType=VARCHAR},"
          + " #{tableMeta.krbConf, jdbcType=VARCHAR},"
          + " #{tableMeta.krbPrincipal, jdbcType=VARCHAR},"
          + " #{tableMeta.properties, typeHandler=com.netease.arctic.server.persistence.converter.Map2StringConverter},"
          + " #{tableMeta.metaVersion}"
          + " )")
  void insertTableMeta(@Param("tableMeta") TableMetadata tableMeta);

  @Delete("DELETE FROM table_metadata WHERE table_id = #{tableId}")
  void deleteTableMetaById(@Param("tableId") long tableId);

  @Update(
      "UPDATE table_metadata SET properties ="
          + " #{tableMeta.properties, typeHandler=com.netease.arctic.server.persistence.converter.Map2StringConverter},"
          + " meta_version=meta_version + 1 "
          + " WHERE table_id = #{tableId} and meta_version = #{tableMeta.metaVersion} ")
  int commitTableChange(
      @Param("tableId") long tableId, @Param("tableMeta") TableMetadata tableMeta);

  @Select(
      "SELECT i.table_id, i.table_name, i.db_name, i.catalog_name, i.format, primary_key, "
          + "table_location, base_location, change_location, meta_store_site, hdfs_site, core_site, "
          + "auth_method, hadoop_username, krb_keytab, krb_conf, krb_principal, properties, meta_version FROM "
          + "table_metadata m INNER JOIN table_identifier i ON m.table_id = i.table_id "
          + "WHERE m.table_id = #{tableId}")
  @Results({
    @Result(property = "tableIdentifier.id", column = "table_id"),
    @Result(property = "tableIdentifier.catalog", column = "catalog_name"),
    @Result(property = "tableIdentifier.database", column = "db_name"),
    @Result(property = "tableIdentifier.tableName", column = "table_name"),
    @Result(property = "tableIdentifier.format", column = "format"),
    @Result(property = "primaryKey", column = "primary_key"),
    @Result(property = "tableLocation", column = "table_location"),
    @Result(property = "baseLocation", column = "base_location"),
    @Result(property = "changeLocation", column = "change_location"),
    @Result(property = "metaStoreSite", column = "meta_store_site"),
    @Result(property = "hdfsSite", column = "hdfs_site"),
    @Result(property = "coreSite", column = "core_site"),
    @Result(property = "authMethod", column = "auth_method"),
    @Result(property = "hadoopUsername", column = "hadoop_username"),
    @Result(property = "krbKeytab", column = "krb_keytab"),
    @Result(property = "krbConf", column = "krb_conf"),
    @Result(property = "krbPrincipal", column = "krb_principal"),
    @Result(
        property = "properties",
        column = "properties",
        typeHandler = Map2StringConverter.class),
    @Result(property = "metaVersion", column = "meta_version")
  })
  TableMetadata selectTableMetaById(@Param("tableId") long tableId);

  @Select(
      "SELECT table_identifier.table_id as table_id, table_identifier.catalog_name as catalog_name,"
          + " table_identifier.db_name as db_name, table_identifier.table_name as table_name, table_identifier.format, "
          + " primary_key,"
          + " table_location, base_location, change_location, meta_store_site, hdfs_site, core_site, auth_method,"
          + " hadoop_username, krb_keytab, krb_conf, krb_principal, properties, meta_version "
          + " FROM table_metadata INNER JOIN table_identifier ON table_metadata.table_id = table_identifier.table_id"
          + " WHERE table_identifier.catalog_name = #{catalogName} and table_identifier.db_name = #{databaseName}"
          + " AND table_identifier.table_name = #{tableName}")
  @Results({
    @Result(property = "tableIdentifier.id", column = "table_id"),
    @Result(property = "tableIdentifier.catalog", column = "catalog_name"),
    @Result(property = "tableIdentifier.database", column = "db_name"),
    @Result(property = "tableIdentifier.tableName", column = "table_name"),
    @Result(property = "tableIdentifier.format", column = "format"),
    @Result(property = "primaryKey", column = "primary_key"),
    @Result(property = "tableLocation", column = "table_location"),
    @Result(property = "baseLocation", column = "base_location"),
    @Result(property = "changeLocation", column = "change_location"),
    @Result(property = "metaStoreSite", column = "meta_store_site"),
    @Result(property = "hdfsSite", column = "hdfs_site"),
    @Result(property = "coreSite", column = "core_site"),
    @Result(property = "authMethod", column = "auth_method"),
    @Result(property = "hadoopUsername", column = "hadoop_username"),
    @Result(property = "krbKeytab", column = "krb_keytab"),
    @Result(property = "krbConf", column = "krb_conf"),
    @Result(property = "krbPrincipal", column = "krb_principal"),
    @Result(
        property = "properties",
        column = "properties",
        typeHandler = Map2StringConverter.class),
    @Result(property = "metaVersion", column = "meta_version")
  })
  TableMetadata selectTableMetaByName(
      @Param("catalogName") String catalogName,
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName);

  @Insert(
      "INSERT INTO table_identifier(catalog_name, db_name, table_name, format) VALUES("
          + " #{tableIdentifier.catalog}, #{tableIdentifier.database}, #{tableIdentifier.tableName}, "
          + " #{tableIdentifier.format})")
  @Options(useGeneratedKeys = true, keyProperty = "tableIdentifier.id")
  void insertTable(@Param("tableIdentifier") ServerTableIdentifier tableIdentifier);

  @Delete("DELETE FROM table_identifier WHERE table_id = #{tableId}")
  Integer deleteTableIdById(@Param("tableId") long tableId);

  @Delete(
      "DELETE FROM table_identifier WHERE catalog_name = #{catalogName} AND db_name = #{databaseName}"
          + " AND table_name = #{tableName}")
  Integer deleteTableIdByName(
      @Param("catalogName") String catalogName,
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName);

  @Select(
      "SELECT table_id, catalog_name, db_name, table_name, format FROM table_identifier"
          + " WHERE catalog_name = #{catalogName} AND db_name = #{databaseName} AND table_name = #{tableName}")
  @Results({
    @Result(property = "id", column = "table_id"),
    @Result(property = "tableName", column = "table_name"),
    @Result(property = "database", column = "db_name"),
    @Result(property = "catalog", column = "catalog_name"),
    @Result(property = "format", column = "format"),
  })
  ServerTableIdentifier selectTableIdentifier(
      @Param("catalogName") String catalogName,
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName);

  @Select(
      "SELECT table_id, catalog_name, db_name, table_name, format FROM table_identifier"
          + " WHERE catalog_name = #{catalogName} AND db_name = #{databaseName}")
  @Results({
    @Result(property = "id", column = "table_id"),
    @Result(property = "catalog", column = "catalog_name"),
    @Result(property = "database", column = "db_name"),
    @Result(property = "tableName", column = "table_name"),
    @Result(property = "format", column = "format")
  })
  List<ServerTableIdentifier> selectTableIdentifiersByDb(
      @Param("catalogName") String catalogName, @Param("databaseName") String databaseName);

  @Select(
      "SELECT table_id, catalog_name, db_name, table_name, format FROM table_identifier"
          + " WHERE catalog_name = #{catalogName}")
  @Results({
    @Result(property = "id", column = "table_id"),
    @Result(property = "catalog", column = "catalog_name"),
    @Result(property = "database", column = "db_name"),
    @Result(property = "tableName", column = "table_name"),
    @Result(property = "format", column = "format")
  })
  List<ServerTableIdentifier> selectTableIdentifiersByCatalog(
      @Param("catalogName") String catalogName);

  @Select("SELECT table_id, catalog_name, db_name, table_name, format FROM table_identifier")
  @Results({
    @Result(property = "id", column = "table_id"),
    @Result(property = "catalog", column = "catalog_name"),
    @Result(property = "database", column = "db_name"),
    @Result(property = "tableName", column = "table_name"),
    @Result(property = "format", column = "format")
  })
  List<ServerTableIdentifier> selectAllTableIdentifiers();

  @Delete("DELETE FROM table_runtime WHERE table_id = #{tableId}")
  void deleteOptimizingRuntime(@Param("tableId") long tableId);

  @Insert(
      "INSERT INTO table_runtime (table_id, catalog_name, db_name, table_name, current_snapshot_id,"
          + " current_change_snapshotId, last_optimized_snapshotId, last_optimized_change_snapshotId,"
          + " last_major_optimizing_time, last_minor_optimizing_time,"
          + " last_full_optimizing_time, optimizing_status, optimizing_status_start_time, optimizing_process_id,"
          + " optimizer_group, table_config, pending_input) VALUES"
          + " (#{runtime.tableIdentifier.id}, #{runtime.tableIdentifier.catalog},"
          + " #{runtime.tableIdentifier.database}, #{runtime.tableIdentifier.tableName}, #{runtime"
          + ".currentSnapshotId},"
          + " #{runtime.currentChangeSnapshotId}, #{runtime.lastOptimizedSnapshotId},"
          + " #{runtime.lastOptimizedChangeSnapshotId}, #{runtime.lastMajorOptimizingTime,"
          + " typeHandler=com.netease.arctic.server.persistence.converter.Long2TsConverter},"
          + " #{runtime.lastMinorOptimizingTime,"
          + " typeHandler=com.netease.arctic.server.persistence.converter.Long2TsConverter},"
          + " #{runtime.lastFullOptimizingTime,"
          + " typeHandler=com.netease.arctic.server.persistence.converter.Long2TsConverter},"
          + " #{runtime.stage},"
          + " #{runtime.currentStatusStartTime, "
          + " typeHandler=com.netease.arctic.server.persistence.converter.Long2TsConverter},"
          + " #{runtime.processId}, #{runtime.optimizerGroup},"
          + " #{runtime.tableConfiguration,"
          + " typeHandler=com.netease.arctic.server.persistence.converter.JsonObjectConverter},"
          + " #{runtime.pendingInput, jdbcType=VARCHAR,"
          + " typeHandler=com.netease.arctic.server.persistence.converter.JsonObjectConverter})")
  void insertTableRuntime(@Param("runtime") DefaultTableRuntime runtime);

  @Update(
      "UPDATE table_runtime SET"
          + " table_config = #{tableConfiguration,"
          + " typeHandler=com.netease.arctic.server.persistence.converter.JsonObjectConverter},"
          + " WHERE table_id = #{tableId}")
  void updateTableConfiguration(
      @Param("tableId") long tableId,
      @Param("tableConfiguration") TableConfiguration tableConfiguration);

  @Update(
      "UPDATE table_runtime SET"
          + " pending_input = #{pendingInput,"
          + " typeHandler=com.netease.arctic.server.persistence.converter.JsonObjectConverter},"
          + " WHERE table_id = #{tableId}")
  void updatePendingInput(
      @Param("tableId") long tableId, @Param("pendingInput") PendingInput pendingInput);

  @Select(
      "SELECT a.table_id, a.catalog_name, a.db_name, a.table_name, i.format, a.current_snapshot_id, a"
          + ".current_change_snapshotId, a.last_optimized_snapshotId, a.last_optimized_change_snapshotId,"
          + " a.last_major_optimizing_time, a.last_minor_optimizing_time, a.last_full_optimizing_time, a.optimizing_status,"
          + " a.optimizing_status_start_time, a.optimizing_process_id,"
          + " a.optimizer_group, a.table_config, a.pending_input, b.optimizing_type, b.target_snapshot_id,"
          + " b.target_change_snapshot_id, b.plan_time, b.from_sequence, b.to_sequence FROM table_runtime a"
          + " INNER JOIN table_identifier i ON a.table_id = i.table_id "
          + " LEFT JOIN table_optimizing_process b ON a.optimizing_process_id = b.process_id")
  @Results({
    @Result(property = "tableId", column = "table_id"),
    @Result(property = "catalogName", column = "catalog_name"),
    @Result(property = "dbName", column = "db_name"),
    @Result(property = "tableName", column = "table_name"),
    @Result(property = "format", column = "format"),
    @Result(property = "currentSnapshotId", column = "current_snapshot_id"),
    @Result(property = "currentChangeSnapshotId", column = "current_change_snapshotId"),
    @Result(property = "lastOptimizedSnapshotId", column = "last_optimized_snapshotId"),
    @Result(
        property = "lastOptimizedChangeSnapshotId",
        column = "last_optimized_change_snapshotId"),
    @Result(
        property = "lastMajorOptimizingTime",
        column = "last_major_optimizing_time",
        typeHandler = Long2TsConverter.class),
    @Result(
        property = "lastMinorOptimizingTime",
        column = "last_minor_optimizing_time",
        typeHandler = Long2TsConverter.class),
    @Result(
        property = "lastFullOptimizingTime",
        column = "last_full_optimizing_time",
        typeHandler = Long2TsConverter.class),
    @Result(property = "tableStatus", column = "optimizing_status"),
    @Result(
        property = "currentStatusStartTime",
        column = "optimizing_status_start_time",
        typeHandler = Long2TsConverter.class),
    @Result(property = "optimizingProcessId", column = "optimizing_process_id"),
    @Result(property = "optimizerGroup", column = "optimizer_group"),
    @Result(
        property = "tableConfig",
        column = "table_config",
        typeHandler = JsonObjectConverter.class),
    @Result(
        property = "pendingInput",
        column = "pending_input",
        typeHandler = JsonObjectConverter.class),
    @Result(property = "optimizingType", column = "optimizing_type"),
    @Result(property = "targetSnapshotId", column = "target_snapshot_id"),
    @Result(property = "targetChangeSnapshotId", column = "target_change_napshot_id"),
    @Result(property = "planTime", column = "plan_time", typeHandler = Long2TsConverter.class),
    @Result(
        property = "fromSequence",
        column = "from_sequence",
        typeHandler = MapLong2StringConverter.class),
    @Result(
        property = "toSequence",
        column = "to_sequence",
        typeHandler = MapLong2StringConverter.class)
  })
  List<TableRuntimePersistency> selectTableRuntimeMetas();
}
