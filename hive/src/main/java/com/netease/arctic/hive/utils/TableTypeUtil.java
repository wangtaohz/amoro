package com.netease.arctic.hive.utils;

import com.netease.arctic.hive.HiveTableProperties;
import com.netease.arctic.hive.table.SupportHive;
import com.netease.arctic.table.ArcticTable;

public class TableTypeUtil {
  public static boolean isHive(ArcticTable arcticTable) {
    return arcticTable instanceof SupportHive;
  }

  public static boolean isFullSnapshotHiveTable(ArcticTable arcticTable) {
    return isHive(arcticTable) &&
        HiveTableProperties.BASE_HIVE_PARTITION_PROJECTION_MODE_TAG.equals(
            arcticTable.properties().getOrDefault(HiveTableProperties.BASE_HIVE_PARTITION_PROJECTION,
                HiveTableProperties.BASE_HIVE_PARTITION_PROJECTION_MODE_DEFAULT));
  }
}
