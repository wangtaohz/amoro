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

package com.netease.arctic.spark.writer;

import com.netease.arctic.spark.source.SupportsDynamicOverwrite;
import com.netease.arctic.spark.source.SupportsOverwrite;
import com.netease.arctic.table.UnkeyedTable;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.SupportsWriteInternalRow;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class ArcticUnkeyedSparkOverwriteWriter implements SupportsWriteInternalRow,
    SupportsOverwrite, SupportsDynamicOverwrite {
  public ArcticUnkeyedSparkOverwriteWriter(UnkeyedTable unkeyedTable, StructType schema) {

  }

  @Override
  public DataSourceWriter overwriteDynamicPartitions() {
    return null;
  }

  @Override
  public DataSourceWriter overwrite(Filter[] filters) {
    return null;
  }

  @Override
  public DataWriterFactory<InternalRow> createInternalRowWriterFactory() {
    return null;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {

  }

  @Override
  public void abort(WriterCommitMessage[] messages) {

  }
}