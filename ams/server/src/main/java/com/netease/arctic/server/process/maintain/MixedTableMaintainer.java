/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.server.process.maintain;

import static com.netease.arctic.utils.ArcticTableUtil.BLOB_TYPE_OPTIMIZED_SEQUENCE_EXIST;
import static org.apache.iceberg.relocated.com.google.common.primitives.Longs.min;

import com.netease.arctic.IcebergFileEntry;
import com.netease.arctic.ams.api.config.DataExpirationConfig;
import com.netease.arctic.data.FileNameRules;
import com.netease.arctic.hive.utils.TableTypeUtil;
import com.netease.arctic.scan.TableEntriesScan;
import com.netease.arctic.server.table.DefaultTableRuntime;
import com.netease.arctic.server.utils.HiveLocationUtil;
import com.netease.arctic.server.utils.IcebergTableUtil;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.BaseTable;
import com.netease.arctic.table.ChangeTable;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.UnkeyedTable;
import com.netease.arctic.utils.ArcticTableUtil;
import com.netease.arctic.utils.TablePropertyUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.primitives.Longs;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedTransferQueue;
import java.util.stream.Collectors;

/** Table maintainer for mixed-iceberg and mixed-hive tables. */
public class MixedTableMaintainer implements TableMaintainer {

  private static final Logger LOG = LoggerFactory.getLogger(MixedTableMaintainer.class);

  private final ArcticTable arcticTable;

  private ChangeTableMaintainer changeMaintainer;

  private final BaseTableMaintainer baseMaintainer;

  private final Set<String> changeFiles;

  private final Set<String> baseFiles;

  private final Set<String> hiveFiles;

  public MixedTableMaintainer(ArcticTable arcticTable) {
    this.arcticTable = arcticTable;
    if (arcticTable.isKeyedTable()) {
      ChangeTable changeTable = arcticTable.asKeyedTable().changeTable();
      BaseTable baseTable = arcticTable.asKeyedTable().baseTable();
      changeMaintainer = new ChangeTableMaintainer(changeTable);
      baseMaintainer = new BaseTableMaintainer(baseTable);
      changeFiles =
          Sets.union(
              IcebergTableUtil.getAllContentFilePath(changeTable),
              IcebergTableUtil.getAllStatisticsFilePath(changeTable));
      baseFiles =
          Sets.union(
              IcebergTableUtil.getAllContentFilePath(baseTable),
              IcebergTableUtil.getAllStatisticsFilePath(baseTable));
    } else {
      baseMaintainer = new BaseTableMaintainer(arcticTable.asUnkeyedTable());
      changeFiles = new HashSet<>();
      baseFiles =
          Sets.union(
              IcebergTableUtil.getAllContentFilePath(arcticTable.asUnkeyedTable()),
              IcebergTableUtil.getAllStatisticsFilePath(arcticTable.asUnkeyedTable()));
    }

    if (TableTypeUtil.isHive(arcticTable)) {
      hiveFiles = HiveLocationUtil.getHiveLocation(arcticTable);
    } else {
      hiveFiles = new HashSet<>();
    }
  }

  @Override
  public void cleanOrphanFiles(DefaultTableRuntime tableRuntime) {
    if (changeMaintainer != null) {
      changeMaintainer.cleanOrphanFiles(tableRuntime);
    }
    baseMaintainer.cleanOrphanFiles(tableRuntime);
  }

  @Override
  public void expireSnapshots(DefaultTableRuntime tableRuntime) {
    if (changeMaintainer != null) {
      changeMaintainer.expireSnapshots(tableRuntime);
    }
    baseMaintainer.expireSnapshots(tableRuntime);
  }

  @VisibleForTesting
  protected void expireSnapshots(long mustOlderThan) {
    if (changeMaintainer != null) {
      changeMaintainer.expireSnapshots(mustOlderThan);
    }
    baseMaintainer.expireSnapshots(mustOlderThan);
  }

  @Override
  public void expireData(DefaultTableRuntime tableRuntime) {
    try {
      DataExpirationConfig expirationConfig =
          tableRuntime.getTableConfiguration().getExpiringDataConfig();
      Types.NestedField field =
          arcticTable.schema().findField(expirationConfig.getExpirationField());
      if (!expirationConfig.isValid(field, arcticTable.name())) {
        return;
      }
      ZoneId defaultZone = IcebergTableMaintainer.getDefaultZoneId(field);
      Instant startInstant;
      if (expirationConfig.getSince() == DataExpirationConfig.Since.CURRENT_TIMESTAMP) {
        startInstant = Instant.now().atZone(defaultZone).toInstant();
      } else {
        long latestBaseTs =
            IcebergTableMaintainer.fetchLatestNonOptimizedSnapshotTime(baseMaintainer.getTable());
        long latestChangeTs =
            changeMaintainer == null
                ? Long.MAX_VALUE
                : IcebergTableMaintainer.fetchLatestNonOptimizedSnapshotTime(
                    changeMaintainer.getTable());
        long latestNonOptimizedTs = Longs.min(latestChangeTs, latestBaseTs);

        startInstant = Instant.ofEpochMilli(latestNonOptimizedTs).atZone(defaultZone).toInstant();
      }
      expireDataFrom(expirationConfig, startInstant);
    } catch (Throwable t) {
      LOG.error("Unexpected purge error for table {} ", tableRuntime.getTableIdentifier(), t);
    }
  }

  @VisibleForTesting
  public void expireDataFrom(DataExpirationConfig expirationConfig, Instant instant) {
    long expireTimestamp = instant.minusMillis(expirationConfig.getRetentionTime()).toEpochMilli();
    Types.NestedField field = arcticTable.schema().findField(expirationConfig.getExpirationField());
    LOG.info(
        "Expiring data older than {} in mixed table {} ",
        Instant.ofEpochMilli(expireTimestamp)
            .atZone(IcebergTableMaintainer.getDefaultZoneId(field))
            .toLocalDateTime(),
        arcticTable.name());

    Expression dataFilter =
        IcebergTableMaintainer.getDataExpression(
            arcticTable.schema(), expirationConfig, expireTimestamp);

    Pair<IcebergTableMaintainer.ExpireFiles, IcebergTableMaintainer.ExpireFiles> mixedExpiredFiles =
        mixedExpiredFileScan(expirationConfig, dataFilter, expireTimestamp);

    expireMixedFiles(mixedExpiredFiles.getLeft(), mixedExpiredFiles.getRight(), expireTimestamp);
  }

  private Pair<IcebergTableMaintainer.ExpireFiles, IcebergTableMaintainer.ExpireFiles>
      mixedExpiredFileScan(
          DataExpirationConfig expirationConfig, Expression dataFilter, long expireTimestamp) {
    return arcticTable.isKeyedTable()
        ? keyedExpiredFileScan(expirationConfig, dataFilter, expireTimestamp)
        : Pair.of(
            new IcebergTableMaintainer.ExpireFiles(),
            getBaseMaintainer().expiredFileScan(expirationConfig, dataFilter, expireTimestamp));
  }

  private Pair<IcebergTableMaintainer.ExpireFiles, IcebergTableMaintainer.ExpireFiles>
      keyedExpiredFileScan(
          DataExpirationConfig expirationConfig, Expression dataFilter, long expireTimestamp) {
    Map<StructLike, IcebergTableMaintainer.DataFileFreshness> partitionFreshness =
        Maps.newConcurrentMap();

    KeyedTable keyedTable = arcticTable.asKeyedTable();
    ChangeTable changeTable = keyedTable.changeTable();
    BaseTable baseTable = keyedTable.baseTable();

    CloseableIterable<MixedFileEntry> changeEntries =
        CloseableIterable.transform(
            changeMaintainer.fileScan(changeTable, dataFilter, expirationConfig),
            e -> new MixedFileEntry(e.getFile(), e.getTsBound(), true));
    CloseableIterable<MixedFileEntry> baseEntries =
        CloseableIterable.transform(
            baseMaintainer.fileScan(baseTable, dataFilter, expirationConfig),
            e -> new MixedFileEntry(e.getFile(), e.getTsBound(), false));
    IcebergTableMaintainer.ExpireFiles changeExpiredFiles =
        new IcebergTableMaintainer.ExpireFiles();
    IcebergTableMaintainer.ExpireFiles baseExpiredFiles = new IcebergTableMaintainer.ExpireFiles();

    try (CloseableIterable<MixedFileEntry> entries =
        CloseableIterable.withNoopClose(
            com.google.common.collect.Iterables.concat(changeEntries, baseEntries))) {
      Queue<MixedFileEntry> fileEntries = new LinkedTransferQueue<>();
      entries.forEach(
          e -> {
            if (IcebergTableMaintainer.mayExpired(e, partitionFreshness, expireTimestamp)) {
              fileEntries.add(e);
            }
          });
      fileEntries
          .parallelStream()
          .filter(
              e -> IcebergTableMaintainer.willNotRetain(e, expirationConfig, partitionFreshness))
          .forEach(
              e -> {
                if (e.isChange()) {
                  changeExpiredFiles.addFile(e);
                } else {
                  baseExpiredFiles.addFile(e);
                }
              });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return Pair.of(changeExpiredFiles, baseExpiredFiles);
  }

  private void expireMixedFiles(
      IcebergTableMaintainer.ExpireFiles changeFiles,
      IcebergTableMaintainer.ExpireFiles baseFiles,
      long expireTimestamp) {
    Optional.ofNullable(changeMaintainer)
        .ifPresent(c -> c.expireFiles(changeFiles, expireTimestamp));
    Optional.ofNullable(baseMaintainer).ifPresent(c -> c.expireFiles(baseFiles, expireTimestamp));
  }

  @Override
  public void autoCreateTags(DefaultTableRuntime tableRuntime) {
    throw new UnsupportedOperationException("Mixed table doesn't support auto create tags");
  }

  @Override
  public Map<String, String> getSummary() {
    Map<String, String> summary = baseMaintainer.getSummary();
    if (changeMaintainer != null) {
      summary.putAll(changeMaintainer.getSummary());
    }
    return summary;
  }

  protected void cleanContentFiles(long lastTime) {
    if (changeMaintainer != null) {
      changeMaintainer.cleanContentFiles(lastTime);
    }
    baseMaintainer.cleanContentFiles(lastTime);
  }

  protected void cleanMetadata(long lastTime) {
    if (changeMaintainer != null) {
      changeMaintainer.cleanMetadata(lastTime);
    }
    baseMaintainer.cleanMetadata(lastTime);
  }

  protected void cleanDanglingDeleteFiles() {
    if (changeMaintainer != null) {
      changeMaintainer.cleanDanglingDeleteFiles();
    }
    baseMaintainer.cleanDanglingDeleteFiles();
  }

  public ChangeTableMaintainer getChangeMaintainer() {
    return changeMaintainer;
  }

  public BaseTableMaintainer getBaseMaintainer() {
    return baseMaintainer;
  }

  public class ChangeTableMaintainer extends IcebergTableMaintainer {

    private static final int DATA_FILE_LIST_SPLIT = 3000;

    private final UnkeyedTable unkeyedTable;

    public ChangeTableMaintainer(UnkeyedTable unkeyedTable) {
      super(unkeyedTable, new ChangeStoreMaintainerSummaryCollector());
      this.unkeyedTable = unkeyedTable;
    }

    @Override
    public Set<String> orphanFileCleanNeedToExcludeFiles() {
      return Sets.union(changeFiles, Sets.union(baseFiles, hiveFiles));
    }

    @Override
    @VisibleForTesting
    void expireSnapshots(long mustOlderThan) {
      expireFiles(mustOlderThan);
      super.expireSnapshots(mustOlderThan);
    }

    @Override
    public void expireSnapshots(DefaultTableRuntime tableRuntime) {
      if (!expireSnapshotEnabled(tableRuntime)) {
        return;
      }
      long now = System.currentTimeMillis();
      expireFiles(now - snapshotsKeepTime(tableRuntime));
      expireSnapshots(mustOlderThan(tableRuntime, now));
    }

    @Override
    protected long mustOlderThan(DefaultTableRuntime tableRuntime, long now) {
      return min(
          // The change data ttl time
          now - snapshotsKeepTime(tableRuntime),
          // The latest flink committed snapshot should not be expired for recovering flink job
          fetchLatestFlinkCommittedSnapshotTime(table));
    }

    @Override
    protected Set<String> expireSnapshotNeedToExcludeFiles() {
      return Sets.union(baseFiles, hiveFiles);
    }

    @Override
    protected long snapshotsKeepTime(DefaultTableRuntime tableRuntime) {
      return tableRuntime.getTableConfiguration().getChangeDataTTLMinutes() * 60 * 1000;
    }

    public void expireFiles(long ttlPoint) {
      List<IcebergFileEntry> expiredDataFileEntries = getExpiredDataFileEntries(ttlPoint);
      deleteChangeFile(expiredDataFileEntries);
    }

    private List<IcebergFileEntry> getExpiredDataFileEntries(long ttlPoint) {
      TableEntriesScan entriesScan =
          TableEntriesScan.builder(unkeyedTable).includeFileContent(FileContent.DATA).build();
      List<IcebergFileEntry> changeTTLFileEntries = new ArrayList<>();

      try (CloseableIterable<IcebergFileEntry> entries = entriesScan.entries()) {
        entries.forEach(
            entry -> {
              Snapshot snapshot = unkeyedTable.snapshot(entry.getSnapshotId());
              if (snapshot == null || snapshot.timestampMillis() < ttlPoint) {
                changeTTLFileEntries.add(entry);
              }
            });
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close manifest entry scan of " + table.name(), e);
      }
      return changeTTLFileEntries;
    }

    private void deleteChangeFile(List<IcebergFileEntry> expiredDataFileEntries) {
      KeyedTable keyedTable = arcticTable.asKeyedTable();
      if (CollectionUtils.isEmpty(expiredDataFileEntries)) {
        return;
      }

      StructLikeMap<Long> optimizedSequences = ArcticTableUtil.readOptimizedSequence(keyedTable);
      if (MapUtils.isEmpty(optimizedSequences)) {
        LOG.info("table {} not contains max transaction id", keyedTable.id());
        return;
      }

      Map<String, List<IcebergFileEntry>> partitionDataFileMap =
          expiredDataFileEntries.stream()
              .collect(
                  Collectors.groupingBy(
                      entry -> keyedTable.spec().partitionToPath(entry.getFile().partition()),
                      Collectors.toList()));

      List<DataFile> changeDeleteFiles = new ArrayList<>();
      if (keyedTable.spec().isUnpartitioned()) {
        List<IcebergFileEntry> partitionDataFiles =
            partitionDataFileMap.get(
                keyedTable
                    .spec()
                    .partitionToPath(expiredDataFileEntries.get(0).getFile().partition()));

        Long optimizedSequence = optimizedSequences.get(TablePropertyUtil.EMPTY_STRUCT);
        if (optimizedSequence != null && CollectionUtils.isNotEmpty(partitionDataFiles)) {
          changeDeleteFiles.addAll(
              partitionDataFiles.stream()
                  .filter(
                      entry ->
                          FileNameRules.parseChangeTransactionId(
                                  entry.getFile().path().toString(), entry.getSequenceNumber())
                              <= optimizedSequence)
                  .map(entry -> (DataFile) entry.getFile())
                  .collect(Collectors.toList()));
        }
      } else {
        optimizedSequences.forEach(
            (key, value) -> {
              List<IcebergFileEntry> partitionDataFiles =
                  partitionDataFileMap.get(keyedTable.spec().partitionToPath(key));

              if (CollectionUtils.isNotEmpty(partitionDataFiles)) {
                changeDeleteFiles.addAll(
                    partitionDataFiles.stream()
                        .filter(
                            entry ->
                                FileNameRules.parseChangeTransactionId(
                                        entry.getFile().path().toString(),
                                        entry.getSequenceNumber())
                                    <= value)
                        .map(entry -> (DataFile) entry.getFile())
                        .collect(Collectors.toList()));
              }
            });
      }
      tryClearChangeFiles(changeDeleteFiles);
    }

    private void tryClearChangeFiles(List<DataFile> changeFiles) {
      if (CollectionUtils.isEmpty(changeFiles)) {
        return;
      }
      try {
        for (int startIndex = 0;
            startIndex < changeFiles.size();
            startIndex += DATA_FILE_LIST_SPLIT) {
          int end = Math.min(startIndex + DATA_FILE_LIST_SPLIT, changeFiles.size());
          List<DataFile> tableFiles = changeFiles.subList(startIndex, end);
          LOG.info("{} delete {} change files", unkeyedTable.name(), tableFiles.size());
          if (!tableFiles.isEmpty()) {
            DeleteFiles changeDelete = unkeyedTable.newDelete();
            changeFiles.forEach(changeDelete::deleteFile);
            changeDelete.commit();
          }
          LOG.info(
              "{} change committed, delete {} files, complete {}/{}",
              unkeyedTable.name(),
              tableFiles.size(),
              end,
              changeFiles.size());
        }
        summaryCollector.collect(
            ChangeStoreMaintainerSummaryCollector.SNAPSHOT_EXPIRING_DELETE_EXPIRED_FILE_COUNT,
            String.valueOf(changeFiles.size()));
      } catch (Throwable t) {
        LOG.error(unkeyedTable.name() + " failed to delete change files, ignore", t);
      }
    }
  }

  public class BaseTableMaintainer extends IcebergTableMaintainer {

    public BaseTableMaintainer(UnkeyedTable unkeyedTable) {
      super(unkeyedTable, new BasicMaintainerSummaryCollector());
    }

    @Override
    public Set<String> orphanFileCleanNeedToExcludeFiles() {
      return Sets.union(changeFiles, Sets.union(baseFiles, hiveFiles));
    }

    @Override
    protected long mustOlderThan(DefaultTableRuntime tableRuntime, long now) {
      return min(
          // The snapshots keep time for base store
          now - snapshotsKeepTime(tableRuntime),
          // The snapshot optimizing plan based should not be expired for committing
          fetchOptimizingPlanSnapshotTime(table, tableRuntime),
          // The latest non-optimized snapshot should not be expired for data expiring
          fetchLatestNonOptimizedSnapshotTime(table),
          // The latest flink committed snapshot should not be expired for recovering flink job
          fetchLatestFlinkCommittedSnapshotTime(table),
          // The latest snapshot contains the optimized sequence should not be expired for MOR
          fetchLatestOptimizedSequenceSnapshotTime(table));
    }

    /**
     * When committing a snapshot to the base store of mixed format keyed table, it will store the
     * optimized sequence to the snapshot, and will set a flag to the summary of this snapshot.
     *
     * <p>The optimized sequence will affect the correctness of MOR.
     *
     * @param table table
     * @return time of the latest snapshot with the optimized sequence flag in summary
     */
    private long fetchLatestOptimizedSequenceSnapshotTime(Table table) {
      if (arcticTable.isKeyedTable()) {
        Snapshot snapshot =
            findLatestSnapshotContainsKey(table, BLOB_TYPE_OPTIMIZED_SEQUENCE_EXIST);
        return snapshot == null ? Long.MAX_VALUE : snapshot.timestampMillis();
      } else {
        return Long.MAX_VALUE;
      }
    }

    @Override
    protected Set<String> expireSnapshotNeedToExcludeFiles() {
      return Sets.union(changeFiles, hiveFiles);
    }
  }

  public static class MixedFileEntry extends IcebergTableMaintainer.FileEntry {

    private final boolean isChange;

    MixedFileEntry(ContentFile<?> file, Literal<Long> tsBound, boolean isChange) {
      super(file, tsBound);
      this.isChange = isChange;
    }

    public boolean isChange() {
      return isChange;
    }
  }
}
