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

public class ChangeStoreMaintainerSummaryCollector extends BasicMaintainerSummaryCollector {

  public static final String changeStoreMark = "#change";
  // snapshot expiring
  public static final String SNAPSHOT_EXPIRING_DELETE_EXPIRED_FILE_COUNT =
      Action.EXPIRE_SNAPSHOTS.name() + actionSeparator + "delete_change_ttl_files_count";

  public ChangeStoreMaintainerSummaryCollector() {
    super();
  }

  @Override
  public void collect(String key, String value) {
    summary.put(key + changeStoreMark, value);
  }
}
