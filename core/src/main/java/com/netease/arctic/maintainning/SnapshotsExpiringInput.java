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

import com.netease.arctic.ams.api.Action;
import com.netease.arctic.ams.api.config.TableConfiguration;
import com.netease.arctic.table.ArcticTable;

public class SnapshotsExpiringInput extends BasicMaintainingInput {

  private final long optimizingPlanSnapshotTime;

  public SnapshotsExpiringInput(
      ArcticTable table,
      Action action,
      TableConfiguration tableConfiguration,
      long optimizingPlanSnapshotTime) {
    super(table, action, tableConfiguration);
    this.optimizingPlanSnapshotTime = optimizingPlanSnapshotTime;
  }

  public long getOptimizingPlanSnapshotTime() {
    return optimizingPlanSnapshotTime;
  }
}
