package com.netease.arctic.server.table;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.netease.arctic.ams.api.BlockableOperation;
import com.netease.arctic.ams.api.Blocker;
import com.netease.arctic.ams.api.CatalogMeta;
import com.netease.arctic.ams.api.NoSuchObjectException;
import com.netease.arctic.ams.api.OperationConflictException;
import com.netease.arctic.ams.api.TableIdentifier;
import com.netease.arctic.ams.api.TableMeta;
import com.netease.arctic.server.ArcticManagementConf;
import com.netease.arctic.server.catalog.CatalogBuilder;
import com.netease.arctic.server.catalog.ExternalCatalog;
import com.netease.arctic.server.catalog.InternalCatalog;
import com.netease.arctic.server.catalog.ServerCatalog;
import com.netease.arctic.server.exception.AlreadyExistsException;
import com.netease.arctic.server.exception.IllegalMetadataException;
import com.netease.arctic.server.exception.ObjectNotExistsException;
import com.netease.arctic.server.optimizing.OptimizingStatus;
import com.netease.arctic.server.persistence.PersistentBase;
import com.netease.arctic.server.persistence.mapper.CatalogMetaMapper;
import com.netease.arctic.server.persistence.mapper.TableMetaMapper;
import com.netease.arctic.server.table.blocker.TableBlocker;
import com.netease.arctic.server.utils.Configurations;
import com.netease.arctic.table.ArcticTable;
import org.apache.commons.lang.StringUtils;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DefaultTableService extends PersistentBase implements TableService {

  public static final Logger LOG = LoggerFactory.getLogger(DefaultTableService.class);
  private final long externalCatalogRefreshingInterval;
  private final long blockerTimeout;
  private final Map<String, InternalCatalog> internalCatalogMap = new ConcurrentHashMap<>();
  private final Map<String, ExternalCatalog> externalCatalogMap = new ConcurrentHashMap<>();
  private final Map<ServerTableIdentifier, TableRuntime> tableRuntimeMap = new ConcurrentHashMap<>();
  private volatile boolean started = false;
  private RuntimeHandlerChain headHandler;
  private Timer tableExplorerTimer;

  public DefaultTableService(Configurations configuration) {
    this.externalCatalogRefreshingInterval =
        configuration.getLong(ArcticManagementConf.EXTERNAL_CATALOG_REFRESH_INTERVAL);
    this.blockerTimeout = configuration.getLong(ArcticManagementConf.BLOCKER_TIMEOUT);
  }

  @Override
  public List<CatalogMeta> listCatalogMetas() {
    checkStarted();
    List<CatalogMeta> catalogs = internalCatalogMap.values().stream()
        .map(ServerCatalog::getMetadata).collect(Collectors.toList());
    catalogs.addAll(externalCatalogMap.values().stream()
        .map(ServerCatalog::getMetadata).collect(Collectors.toList()));
    return catalogs;
  }

  @Override
  public CatalogMeta getCatalogMeta(String catalogName) {
    checkStarted();
    return getServerCatalog(catalogName).getMetadata();
  }

  @Override
  public boolean catalogExist(String catalogName) {
    checkStarted();
    return internalCatalogMap.containsKey(catalogName) || externalCatalogMap.containsKey(catalogName);
  }

  @Override
  public void createCatalog(CatalogMeta catalogMeta) {
    checkStarted();
    doAs(CatalogMetaMapper.class, mapper -> mapper.insertCatalog(catalogMeta));
    initServerCatalog(catalogMeta);
  }

  private void initServerCatalog(CatalogMeta catalogMeta) {
    ServerCatalog catalog = CatalogBuilder.buildServerCatalog(catalogMeta);
    if (catalog instanceof InternalCatalog) {
      internalCatalogMap.put(catalogMeta.getCatalogName(), (InternalCatalog) catalog);
    } else {
      externalCatalogMap.put(catalogMeta.getCatalogName(), (ExternalCatalog) catalog);
    }
  }

  @Override
  public void dropCatalog(String catalogName) {
    checkStarted();
    doAsExisted(
        CatalogMetaMapper.class,
        mapper -> mapper.deleteCatalog(catalogName),
        () -> new IllegalMetadataException("Catalog " + catalogName + " has more than one table"));
    internalCatalogMap.remove(catalogName);
    externalCatalogMap.remove(catalogName);
  }

  @Override
  public void updateCatalog(CatalogMeta catalogMeta) {
    checkStarted();
    ServerCatalog catalog = getServerCatalog(catalogMeta.getCatalogName());
    doAs(CatalogMetaMapper.class, mapper -> mapper.updateCatalog(catalogMeta));
    catalog.updateMetadata(catalogMeta);
  }

  @Override
  public TableMetadata loadTableMetadata(TableIdentifier tableIdentifier) {
    validataTableExists(tableIdentifier);
    return Optional.ofNullable(getAs(TableMetaMapper.class, mapper ->
            mapper.selectTableMetaByName(tableIdentifier.getCatalog(),
                tableIdentifier.getDatabase(), tableIdentifier.getTableName())))
        .orElseThrow(() -> new ObjectNotExistsException(tableIdentifier));
  }

  @Override
  public void dropTableMetadata(TableIdentifier tableIdentifier, boolean deleteData) {
    checkStarted();
    validataTableExists(tableIdentifier);
    ServerTableIdentifier serverTableIdentifier = getInternalCatalog(tableIdentifier.getCatalog())
        .dropTable(tableIdentifier.getDatabase(), tableIdentifier.getTableName());
    Optional.ofNullable(tableRuntimeMap.remove(serverTableIdentifier))
        .ifPresent(TableRuntime::dispose);
  }

  @Override
  public void createTable(String catalogName, TableMeta tableMeta) {
    checkStarted();
    validateTableNotExists(tableMeta.getTableIdentifier());

    InternalCatalog catalog = getInternalCatalog(catalogName);
    ServerTableIdentifier tableIdentifier = catalog.createTable(tableMeta);
    ArcticTable table = catalog.loadTable(tableIdentifier.getDatabase(), tableIdentifier.getTableName());
    TableRuntime tableRuntime = new TableRuntime(tableIdentifier, this, table.properties());
    tableRuntimeMap.put(tableIdentifier, tableRuntime);
    if (headHandler != null) {
      headHandler.fireTableAdded(table, tableRuntime);
    }
  }

  @Override
  public void updateTableProperties(ServerTableIdentifier tableIdentifier, Map<String, String> properties) {
    checkStarted();
    Optional.ofNullable(tableRuntimeMap.get(tableIdentifier))
        .orElseThrow(() -> new ObjectNotExistsException(tableIdentifier))
        .tryUpdatingConfig(properties);
  }

  @Override
  public List<String> listDatabases(String catalogName) {
    checkStarted();
    return getServerCatalog(catalogName).listDatabases();
  }

  @Override
  public List<ServerTableIdentifier> listTables() {
    checkStarted();
    return getAs(TableMetaMapper.class, TableMetaMapper::selectAllTableIdentifiers);
  }

  @Override
  public List<ServerTableIdentifier> listTables(String catalogName, String dbName) {
    checkStarted();
    return getAs(TableMetaMapper.class, mapper -> mapper.selectTableIdentifiersByDb(catalogName, dbName));
  }

  @Override
  public void createDatabase(String catalogName, String dbName) {
    checkStarted();
    getInternalCatalog(catalogName).createDatabase(dbName);
  }

  @Override
  public void dropDatabase(String catalogName, String dbName) {
    checkStarted();
    getInternalCatalog(catalogName).dropDatabase(dbName);
  }

  @Override
  public ArcticTable loadTable(ServerTableIdentifier tableIdentifier) {
    checkStarted();
    return getServerCatalog(tableIdentifier.getCatalog())
        .loadTable(tableIdentifier.getDatabase(), tableIdentifier.getTableName());
  }

  @Deprecated
  @Override
  public List<TableMetadata> listTableMetas() {
    checkStarted();
    return getAs(TableMetaMapper.class, TableMetaMapper::selectTableMetas);
  }

  @Deprecated
  @Override
  public List<TableMetadata> listTableMetas(String catalogName, String database) {
    checkStarted();
    return getAs(TableMetaMapper.class, mapper -> mapper.selectTableMetasByDb(catalogName, database));
  }

  @Override
  public boolean tableExist(TableIdentifier tableIdentifier) {
    checkStarted();
    return getServerCatalog(tableIdentifier.getCatalog()).exist(
        tableIdentifier.getDatabase(),
        tableIdentifier.getTableName());
  }

  @Override
  public Blocker block(TableIdentifier tableIdentifier, List<BlockableOperation> operations,
                       Map<String, String> properties) throws OperationConflictException {
    checkStarted();
    return getAndCheckExist(ServerTableIdentifier.of(tableIdentifier))
        .block(operations, properties, blockerTimeout)
        .buildBlocker();
  }

  @Override
  public void releaseBlocker(TableIdentifier tableIdentifier, String blockerId) {
    checkStarted();
    TableRuntime tableRuntime = getRuntime(ServerTableIdentifier.of(tableIdentifier));
    if (tableRuntime != null) {
      tableRuntime.release(blockerId);
    }
  }

  @Override
  public long renewBlocker(TableIdentifier tableIdentifier, String blockerId) throws NoSuchObjectException {
    checkStarted();
    TableRuntime tableRuntime = getRuntime(ServerTableIdentifier.of(tableIdentifier));
    if (tableRuntime == null) {
      throw new NoSuchObjectException(tableIdentifier.toString());
    } else {
      return tableRuntime.renew(blockerId, blockerTimeout);
    }
  }

  @Override
  public List<Blocker> getBlockers(TableIdentifier tableIdentifier) {
    checkStarted();
    return getAndCheckExist(ServerTableIdentifier.of(tableIdentifier))
        .getBlockers().stream().map(TableBlocker::buildBlocker).collect(Collectors.toList());
  }

  private ServerCatalog getServerCatalog(String catalogName) {
    ServerCatalog catalog = Optional.ofNullable((ServerCatalog) internalCatalogMap.get(catalogName))
        .orElse(externalCatalogMap.get(catalogName));
    return Optional.of(catalog).orElseThrow(() -> new ObjectNotExistsException("Catalog " + catalogName));
  }

  private InternalCatalog getInternalCatalog(String catalogName) {
    return Optional.ofNullable(internalCatalogMap.get(catalogName))
        .orElseThrow(() -> new ObjectNotExistsException("InternalCatalog " + catalogName));
  }

  @Override
  public void addHandlerChain(RuntimeHandlerChain handler) {
    checkNotStarted();
    if (headHandler == null) {
      headHandler = handler;
    } else {
      headHandler.appendNext(handler);
    }
  }

  @Override
  public void handleTableChanged(TableRuntime tableRuntime, OptimizingStatus originalStatus) {
    if (headHandler != null) {
      headHandler.fireStatusChanged(tableRuntime, originalStatus);
    }
  }

  @Override
  public void handleTableChanged(TableRuntime tableRuntime, TableConfiguration originalConfig) {
    if (headHandler != null) {
      headHandler.fireConfigChanged(tableRuntime, originalConfig);
    }
  }

  @Override
  public void initialize() {
    checkNotStarted();
    List<CatalogMeta> catalogMetas =
        getAs(CatalogMetaMapper.class, CatalogMetaMapper::getCatalogs);
    catalogMetas.forEach(this::initServerCatalog);

    List<TableRuntimeMeta> tableRuntimeMetaList =
        getAs(TableMetaMapper.class, TableMetaMapper::selectTableRuntimeMetas);
    tableRuntimeMetaList.forEach(tableRuntimeMeta -> {
      TableRuntime tableRuntime = tableRuntimeMeta.constructTableRuntime(this);
      tableRuntimeMap.put(tableRuntime.getTableIdentifier(), tableRuntime);
    });

    if (headHandler != null) {
      headHandler.startHandler(tableRuntimeMetaList);
    }
    tableExplorerTimer = new Timer("ExternalTableExplorer", true);
    tableExplorerTimer.scheduleAtFixedRate(
        new TableExplorer(),
        0,
        externalCatalogRefreshingInterval);
    started = true;
  }

  public TableRuntime getAndCheckExist(ServerTableIdentifier tableIdentifier) {
    TableRuntime tableRuntime = getRuntime(tableIdentifier);
    if (tableRuntime == null) {
      throw new ObjectNotExistsException(tableIdentifier);
    }
    return tableRuntime;
  }

  @Override
  public TableRuntime getRuntime(ServerTableIdentifier tableIdentifier) {
    checkStarted();
    return tableRuntimeMap.get(tableIdentifier);
  }

  @Override
  public boolean contains(ServerTableIdentifier tableIdentifier) {
    checkStarted();
    return tableRuntimeMap.containsKey(tableIdentifier);
  }

  public void dispose() {
    tableExplorerTimer.cancel();
    headHandler.dispose();
  }

  private void validateTableIdentifier(TableIdentifier tableIdentifier) {
    if (StringUtils.isBlank(tableIdentifier.getTableName())) {
      throw new IllegalMetadataException("table name is blank");
    }
    if (StringUtils.isBlank(tableIdentifier.getCatalog())) {
      throw new IllegalMetadataException("catalog is blank");
    }
    if (StringUtils.isBlank(tableIdentifier.getDatabase())) {
      throw new IllegalMetadataException("database is blank");
    }
  }

  private void validateTableNotExists(TableIdentifier tableIdentifier) {
    validateTableIdentifier(tableIdentifier);

    if (tableExist(tableIdentifier)) {
      String exception = String.format("The table is existed, catalog: %s, db: %s, table:%s",
          tableIdentifier.getCatalog(), tableIdentifier.getDatabase(), tableIdentifier.getTableName());

      LOG.warn(exception);
      throw new AlreadyExistsException(tableIdentifier);
    }
  }

  private void validataTableExists(TableIdentifier tableIdentifier) {
    validateTableIdentifier(tableIdentifier);

    if (!tableExist(tableIdentifier)) {
      String exception = String.format("The table is not existed, catalog: %s, db: %s, table:%s",
          tableIdentifier.getCatalog(), tableIdentifier.getDatabase(), tableIdentifier.getTableName());

      LOG.warn(exception);
      throw new ObjectNotExistsException(tableIdentifier);
    }
  }

  private void checkStarted() {
    if (!started) {
      throw new IllegalStateException("Table service has not started yet.");
    }
  }

  private void checkNotStarted() {
    if (started) {
      throw new IllegalStateException("Table service has started.");
    }
  }

  private class TableExplorer extends TimerTask {

    @Override
    public void run() {
      for (ExternalCatalog externalCatalog : externalCatalogMap.values()) {
        try {
          Set<TableIdentity> tableIdentifiers = externalCatalog.listTables().stream()
              .map(TableIdentity::new)
              .collect(Collectors.toSet());
          Map<TableIdentity, ServerTableIdentifier> serverTableIdentifiers =
              getAs(
                  TableMetaMapper.class,
                  mapper -> mapper.selectTableIdentifiersByCatalog(externalCatalog.name())).stream()
                  .collect(Collectors.toMap(TableIdentity::new, tableIdentifier -> tableIdentifier));
          Sets.difference(tableIdentifiers, serverTableIdentifiers.keySet())
              .forEach(tableIdentity -> {
                try {
                  syncTable(externalCatalog, tableIdentity);
                } catch (Exception e) {
                  LOG.error("TableExplorer sync table {} error", tableIdentity.toString(), e);
                }
              });
          Sets.difference(serverTableIdentifiers.keySet(), tableIdentifiers)
              .forEach(tableIdentity -> {
                try {
                  disposeTable(externalCatalog, serverTableIdentifiers.get(tableIdentity));
                } catch (Exception e) {
                  LOG.error("TableExplorer dispose table {} error", tableIdentity.toString(), e);
                }
              });
        } catch (Exception e) {
          LOG.error("TableExplorer run error", e);
        }
      }
    }
  }

  private void syncTable(ExternalCatalog externalCatalog, TableIdentity tableIdentity) {
    ServerTableIdentifier tableIdentifier =
        externalCatalog.syncTable(tableIdentity.getDatabase(), tableIdentity.getTableName());
    try {
      ArcticTable table = externalCatalog.loadTable(tableIdentifier.getDatabase(),
          tableIdentifier.getTableName());
      TableRuntime tableRuntime = new TableRuntime(tableIdentifier, this, table.properties());
      tableRuntimeMap.put(tableIdentifier, tableRuntime);
      if (headHandler != null) {
        headHandler.fireTableAdded(table, tableRuntime);
      }
    } catch (Exception e) {
      disposeTable(externalCatalog, tableIdentifier);
      LOG.error("sync table {} error while build table runtime", tableIdentifier, e);
    }
  }

  private void disposeTable(ExternalCatalog externalCatalog, ServerTableIdentifier tableIdentifier) {
    externalCatalog.disposeTable(tableIdentifier.getDatabase(), tableIdentifier.getTableName());
    Optional.ofNullable(tableRuntimeMap.remove(tableIdentifier))
        .ifPresent(TableRuntime::dispose);
  }

  private static class TableIdentity {

    private final String database;
    private final String tableName;

    protected TableIdentity(TableIdentifier tableIdentifier) {
      this.database = tableIdentifier.getDatabase();
      this.tableName = tableIdentifier.getTableName();
    }

    protected TableIdentity(ServerTableIdentifier serverTableIdentifier) {
      this.database = serverTableIdentifier.getDatabase();
      this.tableName = serverTableIdentifier.getTableName();
    }

    public String getDatabase() {
      return database;
    }

    public String getTableName() {
      return tableName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TableIdentity that = (TableIdentity) o;
      return Objects.equal(database, that.database) && Objects.equal(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(database, tableName);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("database", database)
          .add("tableName", tableName)
          .toString();
    }
  }
}