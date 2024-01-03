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

package com.netease.arctic.maintainning;

import com.netease.arctic.process.TaskExecutor;

import java.util.Map;

public class BasicMaintainingExecutorFactory
    implements MaintainingExecutorFactory<BasicMaintainingInput, BasicMaintainingOutput> {
  @Override
  public void initialize(Map<String, String> properties) {}

  @Override
  public TaskExecutor<BasicMaintainingOutput> createExecutor(BasicMaintainingInput input) {
    switch (input.action()) {
      case EXPIRE_SNAPSHOTS:
        return new SnapshotsExpiringExecutor((SnapshotsExpiringInput) input);
      case CLEAN_ORPHANED_FILES:
        return new OrphanFilesCleaningExecutor(input);
      case CLEAN_DANGLING_DELETE_FILES:
        return new DanglingDeleteFilesCleaningExecutor(input);
      case HIVE_COMMIT_SYNC:
        return new HiveCommitSyncExecutor(input);
      case DATA_EXPIRING:
        return new DataExpiringExecutor(input);
      default:
        throw new UnsupportedOperationException("Unsupported action: " + input.action());
    }
  }
}
