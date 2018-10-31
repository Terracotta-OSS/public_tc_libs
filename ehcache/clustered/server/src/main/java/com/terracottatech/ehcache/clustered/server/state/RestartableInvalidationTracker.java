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

package com.terracottatech.ehcache.clustered.server.state;

import com.terracottatech.ehcache.common.frs.RestartableGenericMap;
import org.ehcache.clustered.server.state.InvalidationTrackerImpl;

public class RestartableInvalidationTracker extends InvalidationTrackerImpl {

  private final RestartableGenericMap<Long, Integer> invalidationMap;

  public RestartableInvalidationTracker(RestartableGenericMap<Long, Integer> invalidationMap) {
    this.invalidationMap = invalidationMap;
  }

  @Override
  public boolean isClearInProgress() {
    return invalidationMap.containsKey(Long.MIN_VALUE);
  }

  @Override
  public void setClearInProgress(boolean clearInProgress) {
    if (clearInProgress) {
      invalidationMap.put(Long.MIN_VALUE, 1);
    } else {
      invalidationMap.remove(Long.MIN_VALUE);
    }
  }

  @Override
  protected RestartableGenericMap<Long, Integer> getInvalidationMap() {
    return invalidationMap;
  }
}
