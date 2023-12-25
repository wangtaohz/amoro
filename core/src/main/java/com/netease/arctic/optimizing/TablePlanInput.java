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

package com.netease.arctic.optimizing;

import com.netease.arctic.table.ArcticTable;
import org.apache.iceberg.expressions.Expressions;

public class TablePlanInput {
  private String ref;
  private long targetSnapshotId;
  private long targetChangeSnapshotId;
  private ArcticTable table;
  private Expressions filter;

  public TablePlanInput(
      String ref,
      long targetSnapshotId,
      long targetChangeSnapshotId,
      ArcticTable table,
      Expressions filter) {
    this.ref = ref;
    this.targetSnapshotId = targetSnapshotId;
    this.targetChangeSnapshotId = targetChangeSnapshotId;
    this.table = table;
    this.filter = filter;
  }

  public String getRef() {
    return ref;
  }

  public long getTargetSnapshotId() {
    return targetSnapshotId;
  }

  public long getTargetChangeSnapshotId() {
    return targetChangeSnapshotId;
  }

  public ArcticTable getTable() {
    return table;
  }

  public Expressions getFilter() {
    return filter;
  }
}
