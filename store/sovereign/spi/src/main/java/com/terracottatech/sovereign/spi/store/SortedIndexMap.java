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
/**
 * Sorted map interface.
 * @author mscott
 */
public interface SortedIndexMap<K extends Comparable<K>, RK extends Comparable<RK>, C extends Context, L extends Locator>
        extends IndexMap<K, RK, C, L> {

  /**
   * Leaves the locator prepped to move forward
   *
   * @param c
   * @param key
   * @return a locator to the first key higher than the given key.
   */
  L higher(C c, K key);

  /**
   * Leaves the locator prepped to move forward
   * @param c
   * @param key
   * @returna locator to the first key higher or equal than the given key.
   */
  L higherEqual(C c, K key);


  /**
   * Moved the locator prepped to move backward
   * @param c
   * @param key
   * @return a locator to the last key lower than given key.
   */
  L lower(C c, K key);

  /**
   *
   * @param c
   * @param key
   * @return
   */
  L lowerEqual(C c, K key);

  /**
   * Leaves the locator prepped to move forward.
   * @param c
   * @return location of the data associated with the first key.
   */
  L first(C c);

  /**
   * Leaves the locator prepped to move backward.
   * @param c
   * @return location of the data associated with the last key in the map.
   */
  L last(C c);
}
