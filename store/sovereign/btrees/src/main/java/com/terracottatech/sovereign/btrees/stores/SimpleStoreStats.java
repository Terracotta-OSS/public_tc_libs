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
package com.terracottatech.sovereign.btrees.stores;

import com.terracottatech.sovereign.common.utils.MiscUtils;

/**
 * Total placeholder stats.
 *
 * @author cschanck
 */
public class SimpleStoreStats {

  private final StatsType type;

  /**
   * The enum Stats type.
   */
  public static enum StatsType {

    DISK,

    MEMORY
  };

  private long userBytes;
  private long allocatedBytes;

  /**
   * Instantiates a new Simple store stats.
   *
   * @param type the type
   */
  public SimpleStoreStats(StatsType type) {
    this.type = type;
  }

  public static SimpleStoreStats memoryStats() {
    return new SimpleStoreStats(StatsType.MEMORY);
  }

  public static SimpleStoreStats diskStats() {
    return new SimpleStoreStats(StatsType.DISK);
  }

  /**
   * Gets user bytes.
   *
   * @return the user bytes
   */
  public long getUserBytes() {
    return userBytes;
  }

  public void incrementUserBytes(long... v) {
    for (long l : v) {
      this.userBytes += l;
    }
  }

  /**
   * Gets allocated bytes.
   *
   * @return the allocated bytes
   */
  public long getAllocatedBytes() {
    return allocatedBytes;
  }

  public void incrementAllocatedBytes(long... v) {
    for (long l : v) {
      this.allocatedBytes += l;
    }
  }

  public long slackBytes() {
    return allocatedBytes - userBytes;
  }

  @Override
  public String toString() {
    return "StoreStats: " + type +
      " allocated=" + MiscUtils.bytesAsNiceString(getAllocatedBytes()) +
      " user=" + MiscUtils.bytesAsNiceString(getUserBytes()) +
      " slack=" + MiscUtils.bytesAsNiceString(slackBytes());
  }
}
