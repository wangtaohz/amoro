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

import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.io.DataTestHelpers;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestFindDuplicateRecords {
  private static final Logger LOG = LoggerFactory.getLogger(TestFindDuplicateRecords.class);
  private final String thriftUrl;
  private final String CID_COLUMN = "id";
  private final String HID_COLUMN = "name";
  
  private final Object CID_VALUE = 2003;
  private final Object HID_VALUE = "hhh";

  public static void main(String[] args) throws IOException {
    TestFindDuplicateRecords scan = new TestFindDuplicateRecords("thrift://localhost:1260");
    scan.test(TableIdentifier.of("local_iceberg", "db", "user10"));
  }

  public TestFindDuplicateRecords(String thriftUrl) {
    this.thriftUrl = thriftUrl;
  }

  public void test(TableIdentifier tableIdentifier) throws IOException {
    ArcticTable arcticTable = loadTable(tableIdentifier);
    Table table = arcticTable.asUnkeyedTable();
    List<ContentFile<?>> relatedFiles = findRelatedFiles(table, table.currentSnapshot().snapshotId());

    System.out.println("===== related data files =====");
    relatedFiles.forEach(f -> System.out.println(f.path()));
  }

  public List<ContentFile<?>> findRelatedFiles(Table table, long snapshotId) throws IOException {
    List<ContentFile<?>> files = new ArrayList<>();
    for (FileScanTask task : table.newScan().useSnapshot(snapshotId).planFiles()) {
      List<Record> records =
          DataTestHelpers.readDataFile(FileFormat.PARQUET, table.schema(), task.file().path().toString());
      for (Record record : records) {
        if (equals((GenericRecord) record)) {
          files.add(task.file());
          break;
        }
      }
    }
    return files;
  }
  
  public static void planFiles(Table table, long snapshotId) {
    String file1 = "xxx";
    String file2 = "yyy";
    for (FileScanTask task : table.newScan().useSnapshot(snapshotId).planFiles()) {
      String path = task.file().path().toString();
      if (path.equals(file1) || path.equals(file2)) {
        System.out.println("===== related data files =====");
        System.out.println(path);
        for (DeleteFile delete : task.deletes()) {
          System.out.println(delete.path());
        }
      }
    }
  }
  
  private boolean equals(GenericRecord record) {
    return record.getField(CID_COLUMN).equals(CID_VALUE) && record.getField(HID_COLUMN).equals(HID_VALUE);
  }

  private ArcticTable loadTable(TableIdentifier tableIdentifier) {
    String catalogUrl = thriftUrl + "/" + tableIdentifier.getCatalog();

    // 1.scan files
    ArcticCatalog load = CatalogLoader.load(catalogUrl);
    return load.loadTable(tableIdentifier);
  }
}
