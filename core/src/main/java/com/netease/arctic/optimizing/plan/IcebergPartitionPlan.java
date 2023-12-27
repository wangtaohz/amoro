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

package com.netease.arctic.optimizing.plan;

import com.netease.arctic.optimizing.IcebergRewriteExecutorFactory;
import com.netease.arctic.optimizing.OptimizingInputProperties;
import com.netease.arctic.server.table.TableRuntime;
import com.netease.arctic.table.ArcticTable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.util.Pair;

import java.util.List;
import java.util.stream.Collectors;

public class IcebergPartitionPlan extends AbstractPartitionPlan {

  protected IcebergPartitionPlan(
      TableRuntime tableRuntime,
      ArcticTable table,
      Pair<Integer, StructLike> partition,
      long planTime) {
    super(tableRuntime, table, partition, planTime);
  }

  @Override
  protected TaskSplitter buildTaskSplitter() {
    return new BinPackingTaskSplitter();
  }

  @Override
  protected OptimizingInputProperties buildTaskProperties() {
    OptimizingInputProperties properties = new OptimizingInputProperties();
    properties.setExecutorFactoryImpl(IcebergRewriteExecutorFactory.class.getName());
    return properties;
  }

  @Override
  protected List<SplitTask> filterSplitTasks(List<SplitTask> splitTasks) {
    return splitTasks.stream().filter(this::enoughInputFiles).collect(Collectors.toList());
  }

  protected boolean enoughInputFiles(SplitTask splitTask) {
    boolean only1DataFile =
        splitTask.getRewriteDataFiles().size() == 1
            && splitTask.getRewritePosDataFiles().size() == 0
            && splitTask.getDeleteFiles().size() == 0;
    return !only1DataFile;
  }
}
