package com.netease.arctic.server.table;

import com.netease.arctic.AmoroTable;
import com.netease.arctic.server.process.optimizing.OptimizingStage;

public interface TableWatcher {

  // TODO wangtaohz: useless?
  void start();

  void tableAdded(TableRuntime tableRuntime, AmoroTable<?> table);

  void tableRemoved(TableRuntime tableRuntime);

  // TODO wangtaohz: useless?
  void tableChanged(TableRuntime tableRuntime, TableConfiguration oldConfig);
}
