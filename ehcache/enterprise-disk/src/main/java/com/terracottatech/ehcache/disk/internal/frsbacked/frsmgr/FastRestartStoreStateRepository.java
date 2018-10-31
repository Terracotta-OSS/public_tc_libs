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
package com.terracottatech.ehcache.disk.internal.frsbacked.frsmgr;

import org.ehcache.spi.persistence.StateHolder;

import com.terracottatech.ehcache.disk.internal.common.RestartableLocalMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * State repository within FRS.
 * <p>
 * One per FRS container where each container is managed by an instance of {@link DefaultRestartableStoreContainerManager}.
 *
 * @author RKAV
 */
class FastRestartStoreStateRepository implements RepositoryLoader {
  private final Map<String, RestartableLocalMap<?, ?>> knownMaps;
  private final RestartableMapProvider mapProvider;

  FastRestartStoreStateRepository(final RestartableMapProvider mapProvider) {
    this.knownMaps = new HashMap<>();
    this.mapProvider = mapProvider;
  }

  /**
   * Loads a reference to the state repository map.
   * <p>
   *   Called during FRS recovery.
   *
   * @param composedId Unique Id representing the state repository map.
   * @param stateMap The recovered map
   * @param <K> Key type of the recovered map
   * @param <V> value type of the recovered map
   */
  @Override
  public synchronized  <K extends Serializable, V extends Serializable> void load(String composedId,
                                                                                  RestartableLocalMap<K, V> stateMap) {
    knownMaps.put(composedId, stateMap);
  }

  /**
   * Closes the given restartable map object as it is no longer required.
   *
   * @param composedId Id of the restartable map
   */
  public synchronized void close(String composedId) {
    knownMaps.remove(composedId);
  }

  @Override
  public synchronized void destroy(String composedId) {
    RestartableLocalMap<?, ?> srMap = knownMaps.remove(composedId);
    if (srMap != null) {
      srMap.clear();
    }
  }

  /**
   * Gets an existing or creates a new restartable map within the FRS store.
   * <p>
   *   Note that the creation is synchronized externally to avoid even small windows where unnecessary creation of
   *   restartable maps is required as creating restartable map is expensive.
   *
   * @param composedId Unique Id representing the state repository map
   * @param keyClass Type of key
   * @param valueClass Type of value
   * @param <K> Key type
   * @param <V> Value type
   * @return created or existing restartable map
   */
  synchronized  <K extends Serializable, V extends Serializable> StateHolder<K, V> getOrCreateRestartableMap(
      String storeId, String composedId, Class<K> keyClass, Class<V> valueClass) {
    RestartableLocalMap<?, ?> foundMap = knownMaps.get(composedId);
    if (foundMap == null) {
      foundMap = mapProvider.createRestartableMap(storeId, composedId, keyClass, valueClass);
      if (foundMap == null) {
        // if it is still null, it means a late bootstrap just happened..look it up again
        foundMap = knownMaps.get(composedId);
        if (foundMap == null) {
          // this should not happen
          throw new AssertionError("Unexpected Error: State Repository Map for " + composedId + " not bootstrapped correctly");
        }
      } else {
        knownMaps.put(composedId, foundMap);
      }
    }
    @SuppressWarnings("unchecked")
    RestartableLocalMap<K, V> mapToReturn = (RestartableLocalMap<K, V>)foundMap;
    return mapToReturn;
  }
}
