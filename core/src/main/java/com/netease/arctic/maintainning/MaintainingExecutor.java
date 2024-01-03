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

/**
 * Maintaining executor used to do some maintenance on tables in optimizers, such as orphan file
 * clean, snapshots expired and hive sync.
 *
 * @param <O>
 */
public interface MaintainingExecutor<O extends TaskExecutor.Output> extends TaskExecutor<O> {}
