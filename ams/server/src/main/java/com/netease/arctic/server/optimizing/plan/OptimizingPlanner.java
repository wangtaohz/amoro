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

package com.netease.arctic.server.optimizing.plan;

import com.clearspring.analytics.util.Lists;
import com.netease.arctic.hive.table.SupportHive;
import com.netease.arctic.server.optimizing.OptimizingType;
import com.netease.arctic.server.optimizing.scan.TableFileScanHelper;
import com.netease.arctic.server.table.TableRuntime;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.utils.TableTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OptimizingPlanner extends OptimizingEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizingPlanner.class);

  private static final int MAX_INPUT_FILE_COUNT_PER_THREAD = 5000;
  private static final long MAX_INPUT_FILE_SIZE_PER_THREAD = 5 * 1024 * 1024 * 1024;

  private final Set<String> pendingPartitions;

  protected long processId;
  // TODO check it
  private final long targetSnapshotId;
  private final double availableCore;
  private final long planTime;
  private OptimizingType optimizingType = OptimizingType.MINOR;
  private final PartitionPlannerFactory partitionPlannerFactory;

  public OptimizingPlanner(TableRuntime tableRuntime, double availableCore) {
    super(tableRuntime);
    this.pendingPartitions = tableRuntime.getPendingInput() == null ?
        new HashSet<>() : tableRuntime.getPendingInput().getPartitions();
    this.targetSnapshotId = tableRuntime.getCurrentSnapshotId();
    this.availableCore = availableCore;
    this.planTime = System.currentTimeMillis();
    this.processId = Math.max(tableRuntime.getNewestProcessId() + 1, this.planTime);
    this.partitionPlannerFactory = new PartitionPlannerFactory(this.arcticTable, this.tableRuntime, this.planTime);
  }

  @Override
  protected PartitionEvaluator buildEvaluator(String partitionPath) {
    return partitionPlannerFactory.buildPartitionPlanner(partitionPath);
  }

  public Map<String, Long> getFromSequence() {
    return partitionEvaluatorMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> ((AbstractPartitionPlan) e.getValue()).getFromSequence()));
  }

  public Map<String, Long> getToSequence() {
    return partitionEvaluatorMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> ((AbstractPartitionPlan) e.getValue()).getToSequence()));
  }

  @Override
  protected TableFileScanHelper.PartitionFilter getPartitionFilter() {
    return pendingPartitions::contains;
  }

  public long getTargetSnapshotId() {
    return targetSnapshotId;
  }

  public List<TaskDescriptor> planTasks() {
    long startTime = System.nanoTime();

    if (!isInitEvaluator) {
      initEvaluator();
    }
    if (!isNecessary()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} === skip planning", tableRuntime.getTableIdentifier());
      }
      return Collections.emptyList();
    }

    List<PartitionEvaluator> evaluators = new ArrayList<>(partitionEvaluatorMap.values());
    Collections.sort(evaluators, Comparator.comparing(evaluator -> evaluator.getCost() * -1));

    double maxInputSize = MAX_INPUT_FILE_SIZE_PER_THREAD * availableCore;
    List<PartitionEvaluator> inputPartitions = Lists.newArrayList();
    long actualInputSize = 0;
    for (int i = 0; i < evaluators.size() && actualInputSize < maxInputSize; i++) {
      PartitionEvaluator evaluator = evaluators.get(i);
      inputPartitions.add(evaluator);
      if (actualInputSize + evaluator.getCost() < maxInputSize) {
        actualInputSize += evaluator.getCost();
      }
    }

    double avgThreadCost = actualInputSize / availableCore;
    List<TaskDescriptor> tasks = Lists.newArrayList();
    for (PartitionEvaluator evaluator : inputPartitions) {
      tasks.addAll(((AbstractPartitionPlan) evaluator).splitTasks((int) (actualInputSize / avgThreadCost)));
    }
    if (evaluators.stream().anyMatch(evaluator -> evaluator.getOptimizingType() == OptimizingType.MAJOR)) {
      optimizingType = OptimizingType.MAJOR;
    }
    long endTime = System.nanoTime();
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} ==== {} plan tasks cost {} ns, {} ms", tableRuntime.getTableIdentifier(),
          getOptimizingType(), endTime - startTime, (endTime - startTime) / 1_000_000);
      LOG.debug("{} {} plan get {} tasks", tableRuntime.getTableIdentifier(), getOptimizingType(), tasks.size());
    }
    return tasks;
  }

  public long getPlanTime() {
    return planTime;
  }

  public OptimizingType getOptimizingType() {
    return optimizingType;
  }

  public long getProcessId() {
    return processId;
  }

  private static class PartitionPlannerFactory {
    private final ArcticTable arcticTable;
    private final TableRuntime tableRuntime;
    private final String hiveLocation;
    private final long planTime;

    public PartitionPlannerFactory(ArcticTable arcticTable, TableRuntime tableRuntime, long planTime) {
      this.arcticTable = arcticTable;
      this.tableRuntime = tableRuntime;
      this.planTime = planTime;
      if (com.netease.arctic.hive.utils.TableTypeUtil.isHive(arcticTable)) {
        this.hiveLocation = (((SupportHive) arcticTable).hiveLocation());
      } else {
        this.hiveLocation = null;
      }
    }

    public PartitionEvaluator buildPartitionPlanner(String partitionPath) {
      if (TableTypeUtil.isIcebergTableFormat(arcticTable)) {
        return new IcebergPartitionPlan(tableRuntime, partitionPath, arcticTable, planTime);
      } else {
        if (com.netease.arctic.hive.utils.TableTypeUtil.isHive(arcticTable)) {
          if (arcticTable.isKeyedTable()) {
            return new HiveKeyedTablePartitionPlan(tableRuntime, arcticTable, partitionPath, hiveLocation, planTime);
          } else {
            return new HiveUnkeyedTablePartitionPlan(tableRuntime, arcticTable, partitionPath, hiveLocation, planTime);
          }
        } else {
          if (arcticTable.isKeyedTable()) {
            return new KeyedTablePartitionPlan(tableRuntime, arcticTable, partitionPath, planTime);
          } else {
            return new UnkeyedTablePartitionPlan(tableRuntime, arcticTable, partitionPath, planTime);
          }
        }
      }
    }
  }
}
