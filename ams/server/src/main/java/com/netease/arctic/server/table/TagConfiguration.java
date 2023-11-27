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

package com.netease.arctic.server.table;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Objects;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.CompatiblePropertyUtil;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;

/** Configuration for auto creating tags. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TagConfiguration {
  // tag.auto-create.enabled
  private boolean autoCreateTag = false;
  // tag.auto-create.daily.tag-format
  private String tagFormat;
  // tag.auto-create.trigger.period
  private Period triggerPeriod;
  // tag.auto-create.trigger.offset.minutes
  private int triggerOffsetMinutes;
  // tag.auto-create.trigger.max-delay.minutes
  private int maxDelayMinutes;

  /** The interval for periodically triggering creating tags */
  public enum Period {
    DAILY("daily") {
      @Override
      public long getTagTriggerTime(LocalDateTime checkTime, int triggerOffsetMinutes) {
        LocalTime offsetTime = LocalTime.ofSecondOfDay(triggerOffsetMinutes * 60L);
        LocalDateTime triggerTime = LocalDateTime.of(checkTime.toLocalDate(), offsetTime);
        return triggerTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
      }
    };

    private final String propertyName;

    Period(String propertyName) {
      this.propertyName = propertyName;
    }

    public String propertyName() {
      return propertyName;
    }

    /**
     * Obtain the trigger time for creating a tag, which is the idea time of the last tag before the
     * check time.
     *
     * <p>For example, when creating a daily tag, the check time is 2022-08-08 11:00:00 and the
     * offset is set to be 5 min, the idea trigger time is 2022-08-08 00:05:00.
     */
    public abstract long getTagTriggerTime(LocalDateTime checkTime, int triggerOffsetMinutes);
  }

  public static TagConfiguration parse(Map<String, String> tableProperties) {
    TagConfiguration tagConfig = new TagConfiguration();
    tagConfig.setAutoCreateTag(
        CompatiblePropertyUtil.propertyAsBoolean(
            tableProperties,
            TableProperties.ENABLE_AUTO_CREATE_TAG,
            TableProperties.ENABLE_AUTO_CREATE_TAG_DEFAULT));
    tagConfig.setTagFormat(
        CompatiblePropertyUtil.propertyAsString(
            tableProperties,
            TableProperties.AUTO_CREATE_TAG_DAILY_FORMAT,
            TableProperties.AUTO_CREATE_TAG_DAILY_FORMAT_DEFAULT));
    tagConfig.setTriggerPeriod(
        Period.valueOf(
            CompatiblePropertyUtil.propertyAsString(
                    tableProperties,
                    TableProperties.AUTO_CREATE_TAG_TRIGGER_PERIOD,
                    TableProperties.AUTO_CREATE_TAG_TRIGGER_PERIOD_DEFAULT)
                .toUpperCase(Locale.ROOT)));
    tagConfig.setTriggerOffsetMinutes(
        CompatiblePropertyUtil.propertyAsInt(
            tableProperties,
            TableProperties.AUTO_CREATE_TAG_TRIGGER_OFFSET_MINUTES,
            TableProperties.AUTO_CREATE_TAG_TRIGGER_OFFSET_MINUTES_DEFAULT));
    tagConfig.setMaxDelayMinutes(
        CompatiblePropertyUtil.propertyAsInt(
            tableProperties,
            TableProperties.AUTO_CREATE_TAG_MAX_DELAY_MINUTES,
            TableProperties.AUTO_CREATE_TAG_MAX_DELAY_MINUTES_DEFAULT));
    return tagConfig;
  }

  public boolean isAutoCreateTag() {
    return autoCreateTag;
  }

  public void setAutoCreateTag(boolean autoCreateTag) {
    this.autoCreateTag = autoCreateTag;
  }

  public String getTagFormat() {
    return tagFormat;
  }

  public void setTagFormat(String tagFormat) {
    this.tagFormat = tagFormat;
  }

  public Period getTriggerPeriod() {
    return triggerPeriod;
  }

  public void setTriggerPeriod(Period triggerPeriod) {
    this.triggerPeriod = triggerPeriod;
  }

  public int getTriggerOffsetMinutes() {
    return triggerOffsetMinutes;
  }

  public void setTriggerOffsetMinutes(int triggerOffsetMinutes) {
    this.triggerOffsetMinutes = triggerOffsetMinutes;
  }

  public int getMaxDelayMinutes() {
    return maxDelayMinutes;
  }

  public void setMaxDelayMinutes(int maxDelayMinutes) {
    this.maxDelayMinutes = maxDelayMinutes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TagConfiguration that = (TagConfiguration) o;
    return autoCreateTag == that.autoCreateTag
        && triggerOffsetMinutes == that.triggerOffsetMinutes
        && maxDelayMinutes == that.maxDelayMinutes
        && Objects.equal(tagFormat, that.tagFormat)
        && triggerPeriod == that.triggerPeriod;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        autoCreateTag, tagFormat, triggerPeriod, triggerOffsetMinutes, maxDelayMinutes);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("autoCreateTag", autoCreateTag)
        .add("tagFormat", tagFormat)
        .add("triggerPeriod", triggerPeriod)
        .add("triggerOffsetMinutes", triggerOffsetMinutes)
        .add("maxDelayMinutes", maxDelayMinutes)
        .toString();
  }
}
