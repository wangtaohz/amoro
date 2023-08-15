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

package com.netease.arctic.utils;

import com.netease.arctic.table.TableProperties;
import org.apache.iceberg.util.PropertyUtil;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class RefUtil {
  public static String getTagFormat(Map<String, String> properties) {
    return PropertyUtil.propertyAsString(properties, TableProperties.AUTO_CREATE_TAG_FORMAT,
        TableProperties.AUTO_CREATE_TAG_FORMAT_DEFAULT);
  }

  public static String getBranchFormat(Map<String, String> properties) {
    return PropertyUtil.propertyAsString(properties, TableProperties.AUTO_CREATE_TAG_BRANCH_FORMAT,
        TableProperties.AUTO_CREATE_TAG_BRANCH_FORMAT_DEFAULT);
  }

  public static String getDayTagName(LocalDate now, Map<String, String> properties) {
    return getDayRefName(now, getTagFormat(properties));
  }

  public static String getDayBranchName(LocalDate now, Map<String, String> properties) {
    return getDayRefName(now, getBranchFormat(properties));
  }

  public static String getDayRefName(LocalDate now, String format) {
    return now.format(DateTimeFormatter.ofPattern(format));
  }

  public static LocalDate getDateOfTag(String name, Map<String, String> properties) {
    return getDateOfRef(name, getTagFormat(properties));
  }

  public static LocalDate getDateOfBranch(String name, Map<String, String> properties) {
    return getDateOfRef(name, getBranchFormat(properties));
  }

  public static LocalDate getDateOfRef(String name, String format) {
    return LocalDate.parse(name, DateTimeFormatter.ofPattern(format));
  }
}
