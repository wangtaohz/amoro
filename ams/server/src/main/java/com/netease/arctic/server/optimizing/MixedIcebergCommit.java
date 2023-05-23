package com.netease.arctic.server.optimizing;

import com.netease.arctic.ams.api.CommitMetaProducer;
import com.netease.arctic.data.DataFileType;
import com.netease.arctic.data.FileNameRules;
import com.netease.arctic.data.IcebergContentFile;
import com.netease.arctic.data.PrimaryKeyedFile;
import com.netease.arctic.hive.HMSClientPool;
import com.netease.arctic.hive.table.SupportHive;
import com.netease.arctic.hive.utils.HivePartitionUtil;
import com.netease.arctic.hive.utils.HiveTableUtil;
import com.netease.arctic.op.OverwriteBaseFiles;
import com.netease.arctic.optimizing.OptimizingInputProperties;
import com.netease.arctic.optimizing.RewriteFilesInput;
import com.netease.arctic.optimizing.RewriteFilesOutput;
import com.netease.arctic.server.exception.OptimizingCommitException;
import com.netease.arctic.server.utils.IcebergTableUtils;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.UnkeyedTable;
import com.netease.arctic.trace.SnapshotSummary;
import com.netease.arctic.utils.TableFileUtil;
import com.netease.arctic.utils.TablePropertyUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.glassfish.jersey.internal.guava.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.netease.arctic.server.ArcticServiceConstants.INVALID_SNAPSHOT_ID;

public class MixedIcebergCommit extends IcebergCommit {

  private static final Logger LOG = LoggerFactory.getLogger(MixedIcebergCommit.class);

  protected ArcticTable table;

  protected Collection<TaskRuntime> tasks;

  protected Long fromSnapshotId;

  protected StructLikeMap<Long> fromSequenceOfPartitions;

  protected StructLikeMap<Long> toSequenceOfPartitions;

  public MixedIcebergCommit(
      ArcticTable table, Collection<TaskRuntime> tasks, Long fromSnapshotId,
      StructLikeMap<Long> fromSequenceOfPartitions, StructLikeMap<Long> toSequenceOfPartitions) {
    super(fromSnapshotId, table, tasks);
    this.table = table;
    this.tasks = tasks;
    this.fromSnapshotId = fromSnapshotId == null ? INVALID_SNAPSHOT_ID : fromSnapshotId;
    this.fromSequenceOfPartitions = fromSequenceOfPartitions;
    this.toSequenceOfPartitions = toSequenceOfPartitions;
  }

  /**
   * Resolve Hive Adapt.
   */
  private List<DataFile> prepareCommit() {
    if (!needMoveFile2Hive()) {
      return null;
    }

    HMSClientPool hiveClient = ((SupportHive) table).getHMSClient();
    Map<String, String> partitionPathMap = new HashMap<>();
    Types.StructType partitionSchema = table.isUnkeyedTable() ?
        table.asUnkeyedTable().spec().partitionType() :
        table.asKeyedTable().baseTable().spec().partitionType();

    List<DataFile> newTargetFiles = new ArrayList<>();
    for (TaskRuntime taskRuntime : tasks) {
      RewriteFilesOutput output = taskRuntime.getOutput();
      DataFile[] dataFiles = output.getDataFiles();
      if (dataFiles == null) {
        continue;
      }

      List<DataFile> targetFiles = Arrays.stream(output.getDataFiles()).collect(Collectors.toList());

      long maxTransactionId = targetFiles.stream()
          .mapToLong(dataFile -> FileNameRules.parseTransactionId(dataFile.path().toString()))
          .max()
          .orElse(0L);

      for (DataFile targetFile : targetFiles) {
        if (partitionPathMap.get(taskRuntime.getPartition()) == null) {
          List<String> partitionValues =
              HivePartitionUtil.partitionValuesAsList(targetFile.partition(), partitionSchema);
          String partitionPath;
          if (table.spec().isUnpartitioned()) {
            try {
              Table hiveTable = ((SupportHive) table).getHMSClient().run(client ->
                  client.getTable(table.id().getDatabase(), table.id().getTableName()));
              partitionPath = hiveTable.getSd().getLocation();
            } catch (Exception e) {
              LOG.error("Get hive table failed", e);
              throw new RuntimeException(e);
            }
          } else {
            String hiveSubdirectory = table.isKeyedTable() ?
                HiveTableUtil.newHiveSubdirectory(maxTransactionId) : HiveTableUtil.newHiveSubdirectory();

            Partition partition = HivePartitionUtil.getPartition(hiveClient, table, partitionValues);
            if (partition == null) {
              partitionPath = HiveTableUtil.newHiveDataLocation(((SupportHive) table).hiveLocation(),
                  table.spec(), targetFile.partition(), hiveSubdirectory);
            } else {
              partitionPath = partition.getSd().getLocation();
            }
          }
          partitionPathMap.put(taskRuntime.getPartition(), partitionPath);
        }

        DataFile finalDataFile = moveTargetFiles(targetFile, partitionPathMap.get(taskRuntime.getPartition()));
        newTargetFiles.add(finalDataFile);
      }
    }
    return newTargetFiles;
  }

  @Override
  public void commit() throws OptimizingCommitException {
    if (tasks.isEmpty()) {
      LOG.info("{} getRuntime no tasks to commit", table.id());
    }
    LOG.info("{} getRuntime tasks to commit with from snapshot id = {}", table.id(),
        fromSnapshotId);

    //In the scene of moving files to hive, the files will be renamed
    List<DataFile> hiveNewDataFiles = prepareCommit();

    Set<DataFile> addedDataFiles = Sets.newHashSet();
    Set<DataFile> removedDataFiles = Sets.newHashSet();
    Set<DeleteFile> addedDeleteFiles = Sets.newHashSet();
    Set<DeleteFile> removedDeleteFiles = Sets.newHashSet();

    StructLikeMap<Long> partitionOptimizedSequence =
        TablePropertyUtil.getPartitionOptimizedSequence(table.asKeyedTable());

    for (TaskRuntime taskRuntime : tasks) {
      RewriteFilesInput input = taskRuntime.getInput();
      StructLike partition = partition(input);

      //Check if the partition version has expired
      if (fileInPartitionNeedSkip(partition, partitionOptimizedSequence, fromSequenceOfPartitions)) {
        toSequenceOfPartitions.remove(partition);
        continue;
      }
      //Only base data file need to remove
      if (input.rewrittenDataFiles() != null) {
        Arrays.stream(input.rewrittenDataFiles())
            .map(s -> (PrimaryKeyedFile) s.asDataFile().internalDataFile())
            .filter(s -> s.type() == DataFileType.BASE_FILE)
            .forEach(removedDataFiles::add);
      }

      //Only position delete need to remove
      if (input.rewrittenDeleteFiles() != null) {
        Arrays.stream(input.rewrittenDeleteFiles())
            .filter(IcebergContentFile::isDeleteFile)
            .map(IcebergContentFile::asDeleteFile)
            .forEach(removedDeleteFiles::add);
      }

      RewriteFilesOutput output = taskRuntime.getOutput();
      if (CollectionUtils.isNotEmpty(hiveNewDataFiles)) {
        addedDataFiles.addAll(hiveNewDataFiles);
      } else if (output.getDataFiles() != null) {
        Collections.addAll(addedDataFiles, output.getDataFiles());
      }

      if (output.getDeleteFiles() != null) {
        Collections.addAll(addedDeleteFiles, output.getDeleteFiles());
      }
    }

    try {
      executeCommit(addedDataFiles, removedDataFiles, addedDeleteFiles, removedDeleteFiles);
    } catch (Exception e) {
      //Only failures to clean files will trigger a retry
      LOG.warn("Optimize commit table {} failed, give up commit.", table.id(), e);

      if (needMoveFile2Hive()) {
        try {
          UnkeyedTable baseArcticTable;
          if (table.isKeyedTable()) {
            baseArcticTable = table.asKeyedTable().baseTable();
          } else {
            baseArcticTable = table.asUnkeyedTable();
          }
          LOG.warn("Optimize commit table {} failed, give up commit and clear files in location.", table.id(), e);
          // only delete data files are produced by major optimize, because the major optimize maybe support hive
          // and produce redundant data files in hive location.(don't produce DeleteFile)
          // minor produced files will be clean by orphan file clean
          Set<String> committedFilePath = getCommittedDataFilesFromSnapshotId(baseArcticTable, fromSnapshotId);
          for (ContentFile<?> addedDataFile : addedDataFiles) {
            deleteUncommittedFile(committedFilePath, addedDataFile);
          }
          for (ContentFile<?> addedDeleteFile : addedDeleteFiles) {
            deleteUncommittedFile(committedFilePath, addedDeleteFile);
          }
        } catch (Exception ex) {
          throw new OptimizingCommitException(
              "An exception was encountered when the commit failed to clear the file",
              true);
        }
      }
      throw new OptimizingCommitException("unexpected commit error ", e);
    }
  }

  private void deleteUncommittedFile(Set<String> committedFilePath, ContentFile<?> majorAddFile) {
    String filePath = TableFileUtil.getUriPath(majorAddFile.path().toString());
    if (!committedFilePath.contains(filePath) && table.io().exists(filePath)) {
      table.io().deleteFile(filePath);
      LOG.warn("Delete orphan file {} when optimize commit failed", filePath);
    }
  }

  private void executeCommit(
      Set<DataFile> addedDataFiles,
      Set<DataFile> removedDataFiles,
      Set<DeleteFile> addedDeleteFiles,
      Set<DeleteFile> removedDeleteFiles) {
    //overwrite files
    OverwriteBaseFiles overwriteBaseFiles = new OverwriteBaseFiles(table.asKeyedTable());
    overwriteBaseFiles.set(SnapshotSummary.SNAPSHOT_PRODUCER, CommitMetaProducer.OPTIMIZE.name());
    overwriteBaseFiles.validateNoConflictingAppends(Expressions.alwaysFalse());
    overwriteBaseFiles.dynamic(false);
    toSequenceOfPartitions.forEach(overwriteBaseFiles::updateOptimizedSequence);
    addedDataFiles.forEach(overwriteBaseFiles::addFile);
    addedDeleteFiles.forEach(overwriteBaseFiles::addFile);
    removedDataFiles.forEach(overwriteBaseFiles::deleteFile);
    overwriteBaseFiles.skipEmptyCommit().commit();

    //remove delete files
    if (CollectionUtils.isNotEmpty(removedDeleteFiles)) {
      RewriteFiles rewriteFiles = table.asKeyedTable().baseTable().newRewrite();
      rewriteFiles.set(SnapshotSummary.SNAPSHOT_PRODUCER, CommitMetaProducer.OPTIMIZE.name());
      rewriteFiles.rewriteFiles(Collections.emptySet(), removedDeleteFiles,
          Collections.emptySet(), Collections.emptySet());
      try {
        rewriteFiles.commit();
      } catch (ValidationException e) {
        LOG.warn("Iceberg RewriteFiles commit failed, but ignore", e);
      }
    }

    LOG.info("{} optimize committed, delete {} files [{} posDelete files], " +
            "add {} new files [{} posDelete files]",
        table.id(), removedDataFiles.size(), removedDeleteFiles.size(), addedDataFiles.size(),
        addedDataFiles.size());
  }

  private DataFile moveTargetFiles(DataFile targetFile, String hiveLocation) {
    String oldFilePath = targetFile.path().toString();
    String newFilePath = TableFileUtil.getNewFilePath(hiveLocation, oldFilePath);

    if (!table.io().exists(newFilePath)) {
      if (!table.io().exists(hiveLocation)) {
        LOG.debug("{} hive location {} does not exist and need to mkdir before rename", table.id(), hiveLocation);
        table.io().mkdirs(hiveLocation);
      }
      table.io().rename(oldFilePath, newFilePath);
      LOG.debug("{} move file from {} to {}", table.id(), oldFilePath, newFilePath);
    }

    // org.apache.iceberg.BaseFile.set
    ((StructLike) targetFile).set(1, newFilePath);
    return targetFile;
  }

  private StructLike partition(RewriteFilesInput input) {
    return input.allFiles()[0].partition();
  }

  private boolean needMoveFile2Hive() {
    return OptimizingInputProperties.parse(
        tasks.stream().findAny().get().getProperties()).getMoveFile2HiveLocation();
  }

  private boolean fileInPartitionNeedSkip(
      StructLike partitionData,
      StructLikeMap<Long> partitionOptimizedSequence,
      StructLikeMap<Long> fromSequenceOfPartitions) {
    Long optimizedSequence = partitionOptimizedSequence.getOrDefault(partitionData, -1L);
    Long fromSequence = fromSequenceOfPartitions.getOrDefault(partitionData, Long.MAX_VALUE);

    return optimizedSequence >= fromSequence;
  }

  private static Set<String> getCommittedDataFilesFromSnapshotId(UnkeyedTable table, Long snapshotId) {
    long currentSnapshotId = IcebergTableUtils.getSnapshotId(table, true);
    if (currentSnapshotId == INVALID_SNAPSHOT_ID) {
      return Collections.emptySet();
    }

    if (snapshotId == INVALID_SNAPSHOT_ID) {
      snapshotId = null;
    }

    Set<String> committedFilePath = new HashSet<>();
    for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(currentSnapshotId, snapshotId, table::snapshot)) {
      for (DataFile dataFile : snapshot.addedDataFiles(table.io())) {
        committedFilePath.add(TableFileUtil.getUriPath(dataFile.path().toString()));
      }
    }

    return committedFilePath;
  }
}