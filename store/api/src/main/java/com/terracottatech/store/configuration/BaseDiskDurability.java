/*
 * Copyright (c) 2012-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.store.configuration;

import java.util.concurrent.TimeUnit;

public class BaseDiskDurability implements DiskDurability {
  /**
   * Static instance of {@link DiskDurability} representing disk durability
   * that is left to the operating system. This is the laxest durability,
   * and delegates disk flushing to operating system.
   *
   * Note that this is a constraint, and the implementation is free to be more
   * conservative.
   */
  public static DiskDurability OS_DETERMINED = new BaseDiskDurability(DiskDurabilityEnum.EVENTUAL);
  /**
   * Static instance of {@link DiskDurability} representing disk durability
   * where every mutative operation is fully synced/fsynched/flushed to disk.
   *
   * Note that this is a constraint, and the implementation is free to be more
   * conservative.
   */
  public static DiskDurability ALWAYS = new BaseDiskDurability(DiskDurabilityEnum.EVERY_MUTATION);
  private final DiskDurabilityEnum durabilityEnum;

  BaseDiskDurability(DiskDurabilityEnum durabilityEnum) {
    this.durabilityEnum = durabilityEnum;
  }

  /**
   * Static factory for timed {@link DiskDurability} instances. The time duration
   * denotes the longest time a mutative change will go without being
   * fully synced/fsynched/flushed to disk.
   *
   * Note that this is a constraint, and the implementation is free to be more
   * conservative.
   * @param duration long duration
   * @param units units of the duration
   * @return timed disk durability
   */
  public static BaseDiskDurability.Timed timed(Long duration, TimeUnit units) {
    return new BaseDiskDurability.Timed(duration, units);
  }

  public DiskDurabilityEnum getDurabilityEnum() {
    return durabilityEnum;
  }

  @Override
  public String toString() {
    return "DiskDurability: " + durabilityEnum;
  }

  @Override
  public DiskDurability mergeConservatively(DiskDurability other) {
    if (getDurabilityEnum().compareTo(other.getDurabilityEnum()) < 0) {
      return this;
    }
    return other;
  }

  public static class Timed extends BaseDiskDurability {
    private final long millisDuration;

    Timed(long duration, TimeUnit units) {
      super(DiskDurabilityEnum.TIMED);
      if (duration <= 0) {
        throw new IllegalArgumentException("Timed duration must be positive");
      }
      this.millisDuration = TimeUnit.MILLISECONDS.convert(duration, units);
    }

    public long getMillisDuration() {
      return millisDuration;
    }

    @Override
    public DiskDurability mergeConservatively(DiskDurability other) {
      int ret = getDurabilityEnum().compareTo(other.getDurabilityEnum());
      if (ret < 0) {
        return this;
      } else if (ret == 0) {
        if (getDurabilityEnum() == DiskDurabilityEnum.TIMED) {
          if (getMillisDuration() < ((Timed) other).getMillisDuration()) {
            return this;
          }
        }
      }
      return other;
    }

    @Override
    public String toString() {
      return super.toString() + "(" + millisDuration + "ms)";
    }
  }

}
