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

package com.netease.arctic.server.process.maintain;

import com.netease.arctic.ams.api.Action;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.util.Map;

public class BasicMaintainerSummaryCollector {
  protected final Map<String, String> summary;

  public static final String actionSeparator = "-";
  // orphan files cleaning
  public static final String ORPHAN_FILES_CLEANING_DELETE_CONTENT_FILES_COUNT =
      Action.CLEAN_ORPHANED_FILES.name() + actionSeparator + "delete_content_files_count";
  public static final String ORPHAN_FILES_CLEANING_DELETE_METADATA_FILES_COUNT =
      Action.CLEAN_ORPHANED_FILES.name() + actionSeparator + "delete_metadata_files_count";
  // dangling delete files cleaning
  public static final String DANGLING_DELETES_CLEANING_DELETE_DANGLING_COUNT =
      Action.CLEAN_DANGLING_DELETE_FILES.name()
          + actionSeparator
          + "delete_dangling_delete_files_count";
  // snapshot expiring
  public static final String SNAPSHOT_EXPIRING_TO_DELETE_EXPIRED_FILE_COUNT =
      Action.EXPIRE_SNAPSHOTS.name() + actionSeparator + "to_delete_expired_files_count";
  public static final String SNAPSHOT_EXPIRING_DELETE_EXPIRED_FILE_COUNT =
      Action.EXPIRE_SNAPSHOTS.name() + actionSeparator + "delete_expired_files_count";
  // data expiring
  public static final String DATA_EXPIRING_DELETE_EXPIRED_DATAFILES_COUNT =
      Action.DATA_EXPIRING.name() + actionSeparator + "delete_expired_datafiles_count";
  public static final String DATA_EXPIRING_DELETE_EXPIRED_DELETE_FILES_COUNT =
      Action.DATA_EXPIRING.name() + actionSeparator + "delete_expired_delete_files_count";

  public BasicMaintainerSummaryCollector() {
    summary = Maps.newHashMap();
  }

  public void collect(String key, String value) {
    summary.put(key, value);
  }

  public Map<String, String> buildSummary() {
    return summary;
  }
}
