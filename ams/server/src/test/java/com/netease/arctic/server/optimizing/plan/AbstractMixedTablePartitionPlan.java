package com.netease.arctic.server.optimizing.plan;

import com.netease.arctic.TableTestHelper;
import com.netease.arctic.catalog.CatalogTestHelper;
import com.netease.arctic.catalog.TableTestBase;
import com.netease.arctic.data.IcebergContentFile;
import com.netease.arctic.io.DataTestHelpers;
import com.netease.arctic.server.dashboard.utils.AmsUtil;
import com.netease.arctic.server.optimizing.OptimizingConfig;
import com.netease.arctic.server.optimizing.scan.KeyedTableFileScanHelper;
import com.netease.arctic.server.optimizing.scan.TableFileScanHelper;
import com.netease.arctic.server.optimizing.scan.UnkeyedTableFileScanHelper;
import com.netease.arctic.server.table.ServerTableIdentifier;
import com.netease.arctic.server.table.TableRuntime;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractMixedTablePartitionPlan extends TableTestBase {

  protected TableRuntime tableRuntime;

  public AbstractMixedTablePartitionPlan(CatalogTestHelper catalogTestHelper,
                                         TableTestHelper tableTestHelper) {
    super(catalogTestHelper, tableTestHelper);
  }

  @Before
  public void mock() {
    tableRuntime = Mockito.mock(TableRuntime.class);
    Mockito.when(tableRuntime.loadTable()).thenReturn(getArcticTable());
    Mockito.when(tableRuntime.getTableIdentifier()).thenReturn(
        ServerTableIdentifier.of(AmsUtil.toTableIdentifier(getArcticTable().id())));
    Mockito.when(tableRuntime.getOptimizingConfig()).thenReturn(getConfig());
  }

  public List<TaskDescriptor> testOptimizeFragmentFiles() {
    ArrayList<Record> newRecords = Lists.newArrayList(
        tableTestHelper().generateTestRecord(1, "111", 0, "2022-01-01T12:00:00"),
        tableTestHelper().generateTestRecord(2, "222", 0, "2022-01-01T12:00:00"),
        tableTestHelper().generateTestRecord(3, "333", 0, "2022-01-01T12:00:00"),
        tableTestHelper().generateTestRecord(4, "444", 0, "2022-01-01T12:00:00")
    );
    long transactionId = beginTransaction();
    DataTestHelpers.writeAndCommitBaseStore(getArcticTable(), transactionId, newRecords, false);

    newRecords = Lists.newArrayList(
        tableTestHelper().generateTestRecord(6, "666", 0, "2022-01-01T12:00:00"),
        tableTestHelper().generateTestRecord(7, "777", 0, "2022-01-01T12:00:00"),
        tableTestHelper().generateTestRecord(8, "888", 0, "2022-01-01T12:00:00"),
        tableTestHelper().generateTestRecord(9, "999", 0, "2022-01-01T12:00:00")
    );
    transactionId = beginTransaction();
    DataTestHelpers.writeAndCommitBaseStore(getArcticTable(), transactionId, newRecords, false);

    TableFileScanHelper tableFileScanHelper = getTableFileScanHelper();
    AbstractPartitionPlan partitionPlan = getPartitionPlan();
    List<TableFileScanHelper.FileScanResult> scan = tableFileScanHelper.scan();
    for (TableFileScanHelper.FileScanResult fileScanResult : scan) {
      partitionPlan.addFile(fileScanResult.file(), fileScanResult.deleteFiles());
    }

    return partitionPlan.splitTasks(0);
  }

  private void assertTask(TaskDescriptor expect, TaskDescriptor actual) {
    Assert.assertEquals(expect.getPartition(), actual.getPartition());
    assertFiles(expect.getInput().rewrittenDeleteFiles(), actual.getInput().rewrittenDeleteFiles());
    assertFiles(expect.getInput().rewrittenDataFiles(), actual.getInput().rewrittenDataFiles());
    assertFiles(expect.getInput().readOnlyDeleteFiles(), actual.getInput().readOnlyDeleteFiles());
    assertFiles(expect.getInput().rePosDeletedDataFiles(), actual.getInput().rePosDeletedDataFiles());
    Assert.assertEquals(expect.properties(), actual.properties());
  }

  private void assertFiles(IcebergContentFile<?>[] expect, IcebergContentFile<?>[] actual) {
    if (expect == null) {
      Assert.assertNull(actual);
      return;
    }
    Assert.assertEquals(expect.length, actual.length);
    Set<String> expectFilesPath =
        Arrays.stream(expect).map(ContentFile::path).map(CharSequence::toString).collect(Collectors.toSet());
    Set<String> actualFilesPath =
        Arrays.stream(actual).map(ContentFile::path).map(CharSequence::toString).collect(Collectors.toSet());
    Assert.assertEquals(expectFilesPath, actualFilesPath);
  }

  protected abstract AbstractPartitionPlan getPartitionPlan();

  protected String getPartition() {
    return isPartitionedTable() ? "op_time_day=2022-01-01" : "";
  }

  private TableFileScanHelper getTableFileScanHelper() {
    if (isKeyedTable()) {
      return new KeyedTableFileScanHelper(getArcticTable().asKeyedTable(),
          getArcticTable().asKeyedTable().baseTable().currentSnapshot().snapshotId(), -1, null, null);
    } else {
      return new UnkeyedTableFileScanHelper(getArcticTable().asUnkeyedTable(),
          getArcticTable().asUnkeyedTable().currentSnapshot().snapshotId());
    }

  }

  private long beginTransaction() {
    if (isKeyedTable()) {
      return getArcticTable().asKeyedTable().beginTransaction("");
    } else {
      return 0;
    }
  }

  private OptimizingConfig getConfig() {
    return OptimizingConfig.parseOptimizingConfig(getArcticTable().properties());
  }
}
