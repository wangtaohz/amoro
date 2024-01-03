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

import java.util.HashMap;
import java.util.Map;

public class BasicMaintainingInput implements MaintainingInput {

  protected final Map<String, String> options = new HashMap<>();
  protected final ArcticTable table;
  protected final Action action;
  protected final TableConfiguration tableConfiguration;

  public BasicMaintainingInput(
      ArcticTable table, Action action, TableConfiguration tableConfiguration) {
    this.table = table;
    this.action = action;
    this.tableConfiguration = tableConfiguration;
  }

  @Override
  public void option(String name, String value) {
    this.options.put(name, value);
  }

  @Override
  public void options(Map<String, String> options) {
    this.options.putAll(options);
  }

  @Override
  public Map<String, String> getOptions() {
    return this.options;
  }

  public ArcticTable table() {
    return table;
  }

  public Action action() {
    return action;
  }

  public TableConfiguration tableConfiguration() {
    return tableConfiguration;
  }
}
