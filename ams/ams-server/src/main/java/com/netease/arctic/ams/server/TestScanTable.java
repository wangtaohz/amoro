package com.netease.arctic.ams.server;

import com.netease.arctic.IcebergFileEntry;
import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.scan.TableEntriesScan;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TestScanTable {
  private static final Logger LOG = LoggerFactory.getLogger(TestScanTable.class);
  private final String thriftUrl;

  public static void main(String[] args) throws IOException {
    TestScanTable scan = new TestScanTable("thrift://localhost:1260");
    scan.checkIndependentDeleteFiles(TableIdentifier.of("xxx", "yyy", "zzz"));
  }

  public TestScanTable(String thriftUrl) {
    this.thriftUrl = thriftUrl;
  }

  public void checkIndependentDeleteFiles(TableIdentifier tableIdentifier) throws IOException {
    if (tableIdentifier == null) {
      return;
    }
    String catalogUrl = thriftUrl + "/" + tableIdentifier.getCatalog();

    // 1.scan files
    ArcticCatalog load = CatalogLoader.load(catalogUrl);
    ArcticTable arcticTable = load.loadTable(tableIdentifier);

    TableScan tableScan = arcticTable.asUnkeyedTable().newScan();
    int scanDataFileCount = 0;
    int scanDeleteFileCount = 0;
    Set<String> scanDeleteFileSet = new HashSet<>();
    try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
      for (FileScanTask fileScanTask : fileScanTasks) {
        LOG.info("scan data file:" + fileScanTask.file().path().toString());
        scanDataFileCount++;
        for (DeleteFile delete : fileScanTask.deletes()) {
          LOG.info("scan delete file[{}]: {}", delete.content(), delete.path().toString());
          scanDeleteFileCount++;
          scanDeleteFileSet.add(delete.path().toString());
        }
      }
    }

    LOG.info("total scan {} data files, {} delete files", scanDataFileCount, scanDeleteFileCount);

    // 2.scan all delete files from entries
    TableEntriesScan entriesScan = TableEntriesScan.builder(arcticTable.asUnkeyedTable())
        .useSnapshot(arcticTable.asUnkeyedTable().currentSnapshot().snapshotId())
        .includeFileContent(FileContent.EQUALITY_DELETES, FileContent.POSITION_DELETES)
        .build();
    int independentFilesCount = 0;
    for (IcebergFileEntry entry : entriesScan.entries()) {
      ContentFile<?> file = entry.getFile();
      if (!scanDeleteFileSet.contains(file.path().toString())) {
        LOG.info("find independent file[{}]: {}", file.content(), file.path().toString());
        independentFilesCount++;
      }
    }
    LOG.info("total get {} independent files", independentFilesCount);
  }

}
