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

package com.netease.arctic.server.dashboard.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netease.arctic.ams.api.CatalogMeta;
import com.netease.arctic.ams.api.TableFormat;
import com.netease.arctic.ams.api.properties.CatalogMetaProperties;
import com.netease.arctic.ams.api.utils.CatalogPropertyUtil;
import com.netease.arctic.server.ArcticManagementConf;
import com.netease.arctic.server.dashboard.PlatformFileManager;
import com.netease.arctic.server.dashboard.model.CatalogRegisterInfo;
import com.netease.arctic.server.dashboard.model.CatalogSettingInfo;
import com.netease.arctic.server.dashboard.model.CatalogSettingInfo.ConfigFileItem;
import com.netease.arctic.server.dashboard.response.OkResponse;
import com.netease.arctic.server.dashboard.utils.PropertiesUtil;
import com.netease.arctic.server.table.TableService;
import com.netease.arctic.table.TableProperties;
import io.javalin.http.Context;
import org.apache.commons.lang.StringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.netease.arctic.ams.api.TableFormat.ICEBERG;
import static com.netease.arctic.ams.api.TableFormat.MIXED_HIVE;
import static com.netease.arctic.ams.api.TableFormat.MIXED_ICEBERG;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.AUTH_CONFIGS_KEY_ACCESS_KEY;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.AUTH_CONFIGS_KEY_HADOOP_USERNAME;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.AUTH_CONFIGS_KEY_KEYTAB;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.AUTH_CONFIGS_KEY_KRB5;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.AUTH_CONFIGS_KEY_PRINCIPAL;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.AUTH_CONFIGS_KEY_SECRET_KEY;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.AUTH_CONFIGS_VALUE_TYPE_AK_SK;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.AUTH_CONFIGS_VALUE_TYPE_KERBEROS;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.AUTH_CONFIGS_VALUE_TYPE_SIMPLE;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.CATALOG_TYPE_AMS;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.CATALOG_TYPE_CUSTOM;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.CATALOG_TYPE_GLUE;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.CATALOG_TYPE_HADOOP;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.CATALOG_TYPE_HIVE;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.KEY_WAREHOUSE;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.STORAGE_CONFIGS_KEY_CORE_SITE;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.STORAGE_CONFIGS_KEY_ENDPOINT;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.STORAGE_CONFIGS_KEY_HDFS_SITE;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.STORAGE_CONFIGS_KEY_HIVE_SITE;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.STORAGE_CONFIGS_KEY_REGION;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.STORAGE_CONFIGS_VALUE_TYPE_HADOOP;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.STORAGE_CONFIGS_VALUE_TYPE_S3;
import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.TABLE_FORMATS;

/**
 * The controller that handles catalog requests.
 */
public class CatalogController {
  private final PlatformFileManager platformFileInfoService;

  private static final String CONFIG_TYPE_STORAGE = "storage-config";
  private static final String CONFIG_TYPE_AUTH = "auth-config";
  // <configuration></configuration>  encoded with base64
  private static final String EMPTY_XML_BASE64 = "PGNvbmZpZ3VyYXRpb24+PC9jb25maWd1cmF0aW9uPg==";

  private static final Map<String, List<String>> CATALOG_REQUIRED_PROPERTIES;
  private static final Set<CatalogDescriptor> VALIDATE_CATALOGS;

  private final TableService tableService;

  static {
    CATALOG_REQUIRED_PROPERTIES = Maps.newHashMap();
    CATALOG_REQUIRED_PROPERTIES.put(CATALOG_TYPE_AMS, Lists.newArrayList(KEY_WAREHOUSE));
    CATALOG_REQUIRED_PROPERTIES.put(CATALOG_TYPE_HADOOP, Lists.newArrayList(CatalogProperties.WAREHOUSE_LOCATION));
    CATALOG_REQUIRED_PROPERTIES.put(CATALOG_TYPE_CUSTOM, Lists.newArrayList(CatalogProperties.CATALOG_IMPL));
  }
  
  static {
    VALIDATE_CATALOGS = Sets.newHashSet();
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_AMS, STORAGE_CONFIGS_VALUE_TYPE_S3, ICEBERG));
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_AMS, STORAGE_CONFIGS_VALUE_TYPE_HADOOP, ICEBERG));
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_AMS, STORAGE_CONFIGS_VALUE_TYPE_HADOOP, MIXED_ICEBERG));
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_HIVE, STORAGE_CONFIGS_VALUE_TYPE_HADOOP, MIXED_ICEBERG));
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_HIVE, STORAGE_CONFIGS_VALUE_TYPE_HADOOP, ICEBERG));
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_HIVE, STORAGE_CONFIGS_VALUE_TYPE_HADOOP, MIXED_HIVE));
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_HADOOP, STORAGE_CONFIGS_VALUE_TYPE_HADOOP, MIXED_ICEBERG));
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_HADOOP, STORAGE_CONFIGS_VALUE_TYPE_HADOOP, ICEBERG));
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_GLUE, STORAGE_CONFIGS_VALUE_TYPE_S3, ICEBERG));
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_CUSTOM, STORAGE_CONFIGS_VALUE_TYPE_S3, ICEBERG));
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_CUSTOM, STORAGE_CONFIGS_VALUE_TYPE_HADOOP, ICEBERG));
    VALIDATE_CATALOGS.add(CatalogDescriptor.of(CATALOG_TYPE_CUSTOM, STORAGE_CONFIGS_VALUE_TYPE_HADOOP, MIXED_ICEBERG));
  }

  public CatalogController(TableService tableService, PlatformFileManager platformFileInfoService) {
    this.tableService = tableService;
    this.platformFileInfoService = platformFileInfoService;
  }

  /**
   * Get catalog Type list
   */
  public void getCatalogTypeList(Context ctx) {

    List<ImmutableMap<String, String>> catalogTypes = new ArrayList<>();
    String valueKey = "value";
    String displayKey = "display";
    catalogTypes.add(ImmutableMap.of(valueKey, CATALOG_TYPE_AMS, displayKey, "Arctic Metastore"));
    catalogTypes.add(ImmutableMap.of(valueKey, CATALOG_TYPE_HIVE, displayKey, "Hive Metastore"));
    catalogTypes.add(ImmutableMap.of(valueKey, CATALOG_TYPE_HADOOP, displayKey, "Hadoop"));
    catalogTypes.add(ImmutableMap.of(valueKey, CATALOG_TYPE_GLUE, displayKey, "Glue"));
    catalogTypes.add(ImmutableMap.of(valueKey, CATALOG_TYPE_CUSTOM, displayKey, "Custom"));
    ctx.json(OkResponse.of(catalogTypes));
  }

  /**
   * Convert server auth config to metaAuthConfig
   */
  private Map<String, String> authConvertFromServerToMeta(
      Map<String, String> serverAuthConfig,
      CatalogMeta oldCatalogMeta) {
    Map<String, String> metaAuthConfig = new HashMap<>();
    String authType = serverAuthConfig.getOrDefault(AUTH_CONFIGS_KEY_TYPE, "SIMPLE").toLowerCase();
    metaAuthConfig.put(AUTH_CONFIGS_KEY_TYPE, authType);
    Map<String, String> oldAuthConfig = new HashMap<>();
    if (oldCatalogMeta != null) {
      oldAuthConfig = oldCatalogMeta.getAuthConfigs();
    }

    if (authType.equals(AUTH_CONFIGS_VALUE_TYPE_SIMPLE)) {
      metaAuthConfig.put(
          AUTH_CONFIGS_KEY_HADOOP_USERNAME,
          serverAuthConfig.get(AUTH_CONFIGS_KEY_HADOOP_USERNAME));
    } else if (authType.equals(AUTH_CONFIGS_VALUE_TYPE_KERBEROS)) {
      String keytabFileId = serverAuthConfig.get(AUTH_CONFIGS_KEY_KEYTAB);
      if (!StringUtils.isEmpty(keytabFileId)) {
        String keytabB64 = platformFileInfoService.getFileContentB64ById(Integer.valueOf(keytabFileId));
        metaAuthConfig.put(AUTH_CONFIGS_KEY_KEYTAB, keytabB64);
      } else {
        metaAuthConfig.put(
            AUTH_CONFIGS_KEY_KEYTAB,
            oldAuthConfig.get(AUTH_CONFIGS_KEY_KEYTAB));
      }

      String krbFileId = serverAuthConfig.get(AUTH_CONFIGS_KEY_KRB5);
      if (!StringUtils.isEmpty(krbFileId)) {
        String krbB64 = platformFileInfoService.getFileContentB64ById(Integer.valueOf(krbFileId));
        metaAuthConfig.put(AUTH_CONFIGS_KEY_KRB5, krbB64);
      } else {
        metaAuthConfig.put(
            AUTH_CONFIGS_KEY_KRB5,
            oldAuthConfig.get(AUTH_CONFIGS_KEY_KRB5));
      }
      metaAuthConfig.put(
          AUTH_CONFIGS_KEY_PRINCIPAL,
          serverAuthConfig.get(AUTH_CONFIGS_KEY_PRINCIPAL));
    } else if (authType.equals(AUTH_CONFIGS_VALUE_TYPE_AK_SK)) {
      metaAuthConfig.put(
          AUTH_CONFIGS_KEY_ACCESS_KEY,
          serverAuthConfig.get(AUTH_CONFIGS_KEY_ACCESS_KEY));
      metaAuthConfig.put(
          AUTH_CONFIGS_KEY_SECRET_KEY,
          serverAuthConfig.get(AUTH_CONFIGS_KEY_SECRET_KEY));
    }
    return metaAuthConfig;
  }

  /**
   * Convert meta auth config to server auth config DTO
   */
  private Map<String, Object> authConvertFromMetaToServer(
      String catalogName,
      Map<String, String> metaAuthConfig) {
    Map<String, Object> serverAuthConfig = new HashMap<>();
    String authType = metaAuthConfig.getOrDefault(
        AUTH_CONFIGS_KEY_TYPE,
        AUTH_CONFIGS_VALUE_TYPE_SIMPLE);
    serverAuthConfig.put(AUTH_CONFIGS_KEY_TYPE, authType.toUpperCase());
    if (AUTH_CONFIGS_VALUE_TYPE_SIMPLE.equals(authType)) {
      serverAuthConfig.put(
          AUTH_CONFIGS_KEY_HADOOP_USERNAME,
          metaAuthConfig.get(AUTH_CONFIGS_KEY_HADOOP_USERNAME));
    } else if (AUTH_CONFIGS_VALUE_TYPE_KERBEROS.equals(authType)) {
      serverAuthConfig.put(
          AUTH_CONFIGS_KEY_PRINCIPAL,
          metaAuthConfig.get(AUTH_CONFIGS_KEY_PRINCIPAL));

      serverAuthConfig.put(AUTH_CONFIGS_KEY_KEYTAB, new ConfigFileItem(
          catalogName + ".keytab",
          constructCatalogConfigFileUrl(catalogName, CONFIG_TYPE_AUTH,
              AUTH_CONFIGS_KEY_KEYTAB.replace("\\.", "-"))));

      serverAuthConfig.put(AUTH_CONFIGS_KEY_KRB5, new ConfigFileItem(
          "krb5.conf",
          constructCatalogConfigFileUrl(catalogName, CONFIG_TYPE_AUTH,
              AUTH_CONFIGS_KEY_KRB5.replace("\\.", "-"))));
    } else if (AUTH_CONFIGS_VALUE_TYPE_AK_SK.equals(authType)) {
      CatalogPropertyUtil.migrateProperty(metaAuthConfig, serverAuthConfig, AUTH_CONFIGS_KEY_ACCESS_KEY);
      CatalogPropertyUtil.migrateProperty(metaAuthConfig, serverAuthConfig, AUTH_CONFIGS_KEY_SECRET_KEY);
    }

    return serverAuthConfig;
  }

  private Map<String, Object> storageConvertFromMetaToServer(String catalogName, Map<String, String> config) {
    Map<String, Object> storageConfig = new HashMap<>();
    String storageType = CatalogPropertyUtil.getCompatibleStorageType(config);
    storageConfig.put(STORAGE_CONFIGS_KEY_TYPE, storageType);
    if (STORAGE_CONFIGS_VALUE_TYPE_HADOOP.equals(storageType)) {
      storageConfig.put(STORAGE_CONFIGS_KEY_CORE_SITE, new ConfigFileItem(
          ArcticManagementConf.CATALOG_CORE_SITE + ".xml",
          constructCatalogConfigFileUrl(catalogName, CONFIG_TYPE_STORAGE,
              STORAGE_CONFIGS_KEY_CORE_SITE.replace("\\.", "-"))));

      storageConfig.put(STORAGE_CONFIGS_KEY_HDFS_SITE, new ConfigFileItem(
          ArcticManagementConf.CATALOG_HDFS_SITE + ".xml",
          constructCatalogConfigFileUrl(catalogName, CONFIG_TYPE_STORAGE,
              STORAGE_CONFIGS_KEY_HDFS_SITE.replace("\\.", "-"))));

      storageConfig.put(STORAGE_CONFIGS_KEY_HIVE_SITE, new ConfigFileItem(
          ArcticManagementConf.CATALOG_HIVE_SITE + ".xml",
          constructCatalogConfigFileUrl(catalogName, CONFIG_TYPE_STORAGE,
              STORAGE_CONFIGS_KEY_HIVE_SITE.replace("\\.", "-"))));
    } else if (STORAGE_CONFIGS_VALUE_TYPE_S3.equals(storageType)) {
      CatalogPropertyUtil.migrateProperty(config, storageConfig, STORAGE_CONFIGS_KEY_REGION);
      CatalogPropertyUtil.migrateProperty(config, storageConfig, STORAGE_CONFIGS_KEY_ENDPOINT);
    }

    return storageConfig;
  }

  /**
   * Construct catalog meta through catalog register info.
   */
  private CatalogMeta constructCatalogMeta(CatalogRegisterInfo info, CatalogMeta oldCatalogMeta) {
    CatalogMeta catalogMeta = new CatalogMeta();
    catalogMeta.setCatalogName(info.getName());
    catalogMeta.setCatalogType(info.getType());
    catalogMeta.setCatalogProperties(
        PropertiesUtil.unionCatalogProperties(info.getTableProperties(), info.getProperties()));
    catalogMeta.getCatalogProperties()
        .put(
            CatalogMetaProperties.TABLE_PROPERTIES_PREFIX + TableProperties.SELF_OPTIMIZING_GROUP,
            info.getOptimizerGroup());
    StringBuilder tableFormats = new StringBuilder();
    try {
      // validate table format
      info.getTableFormatList().forEach(item -> tableFormats.append(TableFormat.valueOf(item).name()));
    } catch (Exception e) {
      throw new RuntimeException("Invalid table format list, " + String.join(",", info.getTableFormatList()));
    }
    catalogMeta.getCatalogProperties().put(CatalogMetaProperties.TABLE_FORMATS, tableFormats.toString());
    catalogMeta.setAuthConfigs(authConvertFromServerToMeta(info.getAuthConfig(), oldCatalogMeta));
    // change fileId to base64Code
    Map<String, String> metaStorageConfig = new HashMap<>();
    String storageType =
        info.getStorageConfig().getOrDefault(STORAGE_CONFIGS_KEY_TYPE, STORAGE_CONFIGS_VALUE_TYPE_HADOOP);
    metaStorageConfig.put(STORAGE_CONFIGS_KEY_TYPE, storageType);
    if (storageType.equals(STORAGE_CONFIGS_VALUE_TYPE_HADOOP)) {
      List<String> metaKeyList = Arrays.asList(
          STORAGE_CONFIGS_KEY_HDFS_SITE,
          STORAGE_CONFIGS_KEY_CORE_SITE,
          STORAGE_CONFIGS_KEY_HIVE_SITE);

      // when update catalog, fileId won't be post when file doesn't been changed!
      int idx;
      boolean fillUseOld = oldCatalogMeta != null;
      for (idx = 0; idx < metaKeyList.size(); idx++) {
        String fileId = info.getStorageConfig()
            .get(metaKeyList.get(idx));
        if (!StringUtils.isEmpty(fileId)) {
          String fileSite = platformFileInfoService.getFileContentB64ById(Integer.valueOf(fileId));
          metaStorageConfig.put(metaKeyList.get(idx), StringUtils.isEmpty(fileSite) ? EMPTY_XML_BASE64 : fileSite);
        } else {
          if (fillUseOld) {
            String fileSite = oldCatalogMeta.getStorageConfigs().get(metaKeyList.get(idx));
            metaStorageConfig.put(metaKeyList.get(idx), StringUtils.isEmpty(fileSite) ? EMPTY_XML_BASE64 : fileSite);
          } else {
            metaStorageConfig.put(metaKeyList.get(idx), EMPTY_XML_BASE64);
          }
        }
      }
    } else if (storageType.equals(STORAGE_CONFIGS_VALUE_TYPE_S3)) {
      if (StringUtils.isNotBlank(info.getStorageConfig().get(STORAGE_CONFIGS_KEY_REGION))) {
        metaStorageConfig.put(STORAGE_CONFIGS_KEY_REGION, info.getStorageConfig().get(STORAGE_CONFIGS_KEY_REGION));
      }
      if (StringUtils.isNotBlank(info.getStorageConfig().get(STORAGE_CONFIGS_KEY_ENDPOINT))) {
        metaStorageConfig.put(STORAGE_CONFIGS_KEY_ENDPOINT, info.getStorageConfig().get(STORAGE_CONFIGS_KEY_ENDPOINT));
      }
    } else {
      throw new RuntimeException("Invalid storage type " + storageType);
    }

    catalogMeta.setStorageConfigs(metaStorageConfig);
    return catalogMeta;
  }

  /**
   * Register catalog to ams.
   */
  public void createCatalog(Context ctx) {
    CatalogRegisterInfo info = ctx.bodyAsClass(CatalogRegisterInfo.class);
    validateCatalogRegisterInfo(info);
    if (tableService.catalogExist(info.getName())) {
      throw new RuntimeException("Duplicate catalog name!");
    }
    CatalogMeta catalogMeta = constructCatalogMeta(info, null);
    tableService.createCatalog(catalogMeta);
    ctx.json(OkResponse.of(""));
  }

  private void validateCatalogRegisterInfo(CatalogRegisterInfo info) {
    Preconditions.checkNotNull(info.getAuthConfig(), "Catalog auth config must not be null");
    Preconditions.checkNotNull(info.getStorageConfig(), "Catalog storage config must not be null");
    Preconditions.checkNotNull(info.getProperties(), "Catalog properties must not be null");
    Preconditions.checkNotNull(info.getTableProperties(), "Catalog table properties must not be null");
    Preconditions.checkArgument(info.getTableFormatList() != null && !info.getTableFormatList().isEmpty(),
        "Catalog table format list must not be empty");

    CatalogDescriptor.of(info).validate();

    List<String> requiredProperties = CATALOG_REQUIRED_PROPERTIES.get(info.getType());
    if (requiredProperties != null && !requiredProperties.isEmpty()) {
      for (String propertyName : requiredProperties) {
        Preconditions.checkArgument(info.getProperties().containsKey(propertyName),
            String.format("Catalog type:%s require property:%s.", info.getType(), propertyName));
      }
    }
  }

  /**
   * Get detail of some catalog.
   */
  public void getCatalogDetail(Context ctx) {
    String catalogName = ctx.pathParam("catalogName");
    CatalogMeta catalogMeta = tableService.getCatalogMeta(catalogName);
    CatalogSettingInfo info = new CatalogSettingInfo();

    if (tableService.catalogExist(catalogName)) {
      info.setName(catalogMeta.getCatalogName());
      // We create ams catalog with type hadoop in v0.3, we should be compatible with it.
      if (CATALOG_TYPE_HADOOP.equals(catalogMeta.getCatalogType()) &&
          !catalogMeta.getCatalogProperties().containsKey(TABLE_FORMATS)) {
        info.setType(CATALOG_TYPE_AMS);
      } else {
        info.setType(catalogMeta.getCatalogType());
      }
      info.setAuthConfig(authConvertFromMetaToServer(catalogName, catalogMeta.getAuthConfigs()));
      info.setStorageConfig(storageConvertFromMetaToServer(catalogName, catalogMeta.getStorageConfigs()));
      // we put the table format single
      String tableFormat = catalogMeta.getCatalogProperties().get(CatalogMetaProperties.TABLE_FORMATS);
      if (StringUtils.isEmpty(tableFormat)) {
        if (catalogMeta.getCatalogType().equals(CATALOG_TYPE_HIVE)) {
          tableFormat = TableFormat.MIXED_HIVE.name();
        } else {
          tableFormat = TableFormat.MIXED_ICEBERG.name();
        }
      }
      info.setTableFormatList(Arrays.asList(tableFormat.split(",")));
      info.setProperties(PropertiesUtil.extractCatalogMetaProperties(catalogMeta.getCatalogProperties()));
      info.setTableProperties(PropertiesUtil.extractTableProperties(catalogMeta.getCatalogProperties()));
      info.setOptimizerGroup(info.getTableProperties().getOrDefault(TableProperties.SELF_OPTIMIZING_GROUP,
          TableProperties.SELF_OPTIMIZING_GROUP_DEFAULT));
      info.getProperties().remove(CatalogMetaProperties.TABLE_FORMATS);
      ctx.json(OkResponse.of(info));
      return;
    }
    ctx.json(OkResponse.of(null));
  }

  /**
   * Get detail of some catalog
   * 1、first check whether there are some tables in catalog.
   */
  public void updateCatalog(Context ctx) {
    CatalogRegisterInfo info = ctx.bodyAsClass(CatalogRegisterInfo.class);
    validateCatalogRegisterInfo(info);
    CatalogMeta optCatalog = tableService.getCatalogMeta(info.getName());

    // check only some item can be modified!
    CatalogMeta catalogMeta = constructCatalogMeta(info, optCatalog);
    tableService.updateCatalog(catalogMeta);
    ctx.json(OkResponse.ok());
  }

  /**
   * Check whether we could delete the catalog
   */
  public void catalogDeleteCheck(Context ctx) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(ctx.pathParam("catalogName")), "Catalog name is empty!");
    int tblCount = tableService.listManagedTables(ctx.pathParam("catalogName")).size();
    ctx.json(OkResponse.of(tblCount == 0));
  }

  /**
   * Delete some catalog and information associate with the catalog
   */
  public void deleteCatalog(Context ctx) {
    String catalogName = ctx.pathParam("catalogName");
    Preconditions.checkArgument(StringUtils.isNotEmpty(ctx.pathParam("catalogName")), "Catalog name is empty!");
    List<String> dbs = tableService.listDatabases(catalogName);
    if (dbs != null && dbs.size() == 0) {
      tableService.dropCatalog(catalogName);
      ctx.json(OkResponse.of("OK"));
    } else {
      throw new RuntimeException("Some tables in catalog!");
    }
  }

  /**
   * Construct a url
   */
  private String constructCatalogConfigFileUrl(String catalogName, String type, String key) {
    return String.format("/ams/v1/catalogs/%s/config/%s/%s", catalogName,
        type, key.replaceAll("\\.", "-"));
  }

  /**
   * Get the config file content
   * uri("/catalogs/{catalogName}/config/{type}/{key}
   */
  public void getCatalogConfFileContent(Context ctx) {
    String catalogName = ctx.pathParam("catalogName");
    String confType = ctx.pathParam("type");
    String configKey = ctx.pathParam("key");
    Preconditions.checkArgument(
        StringUtils.isNotEmpty(catalogName) && StringUtils.isNotEmpty(confType) && StringUtils.isNotEmpty(configKey),
        "Catalog name or auth type or config key is null!");

    CatalogMeta catalogMeta = tableService.getCatalogMeta(catalogName);
    if (CONFIG_TYPE_STORAGE.equalsIgnoreCase(confType)) {
      Map<String, String> storageConfig = catalogMeta.getStorageConfigs();
      String key = configKey.replaceAll("-", "\\.");
      ctx.result(Base64.getDecoder().decode(storageConfig.get(key)));
    } else if (CONFIG_TYPE_AUTH.equalsIgnoreCase(confType)) {
      Map<String, String> storageConfig = catalogMeta.getAuthConfigs();
      String key = configKey.replaceAll("-", "\\.");
      ctx.result(Base64.getDecoder().decode(storageConfig.get(key)));
    } else {
      throw new RuntimeException("Invalid request for " + confType);
    }
  }

  private static class CatalogDescriptor {
    private final String catalogType;
    private final String storageType;
    private final TableFormat tableFormat;

    public CatalogDescriptor(String catalogType, String storageType, TableFormat tableFormat) {
      this.catalogType = catalogType;
      this.storageType = storageType;
      this.tableFormat = tableFormat;
    }

    public static CatalogDescriptor of(String catalogType, String storageType, TableFormat tableFormat) {
      return new CatalogDescriptor(catalogType, storageType, tableFormat);
    }

    public static CatalogDescriptor of(CatalogRegisterInfo info) {
      // only support one table format now
      String tableFormat = info.getTableFormatList().get(0);
      String storageType = info.getStorageConfig().get(STORAGE_CONFIGS_KEY_TYPE);
      String catalogType = info.getType();
      return of(catalogType, storageType, TableFormat.valueOf(tableFormat));
    }

    public void validate() {
      Preconditions.checkArgument(VALIDATE_CATALOGS.contains(this), "Not support " + this);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CatalogDescriptor that = (CatalogDescriptor) o;
      return Objects.equal(catalogType, that.catalogType) &&
          Objects.equal(storageType, that.storageType) && tableFormat == that.tableFormat;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(catalogType, storageType, tableFormat);
    }

    @Override
    public String toString() {
      return String.format("Metastore [%s], Storage Type [%s], Table Format [%s]", catalogType, storageType,
          tableFormat);
    }
  }
}
