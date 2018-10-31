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
package com.terracottatech.sovereign.spi.store;

import com.terracottatech.sovereign.spi.SpaceRuntime;

/**
 * Simple map interface.
 * @author mscott
 * @param <K>
 */
public interface IndexMap<K extends Comparable<K>, RK extends Comparable<RK>, C extends Context, L extends Locator> {

  /**
   * runtime object
   * @return
   */
  SpaceRuntime runtime();

  /**
   *
   * @param c
   * @param key
   * @return pointer to data in a container
   */
  L get(C c, K key);

  /**
   * @param recordKey
   * @param c
   * @param key
   * @param pointer to data in a container
   * @return old pointer associated with the input key. Note that this pointer is almost
   * certainly *not* valid any longer; however, if a non null value is returned, it signifies
   * a replace has taken place.
   */
  L put(RK recordKey, C c, K key, L pointer);

  /**
   * @param recordKey
   * @param c
   * @param key
   * @param pointer to data in a container
   * @return pointer the data of the removed key
   */
  boolean remove(RK recordKey, C c, K key, L pointer);

  /**
   * drop this map structure
   */
  void drop();

  /**
   * Estimate of size, point in time.
   * @return
   */
  long estimateSize();
}
