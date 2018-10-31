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
package com.terracottatech.sovereign.spi;

import com.terracottatech.sovereign.spi.store.DataContainer;

/**
 *Configuration passed to the factory when building a map structure
 *
 * @author mscott
 * @param <K> Type of the key
 */
public interface IndexMapConfig<K> {
  /**
   * Type of map
   * @return true if sorted map e.g. b-tree map
   */
  boolean isSortedMap();
  /**
   * Key type of the map.
   * @return Typing object of the index
   */
  com.terracottatech.store.Type<K> getType();

  DataContainer<?, ?, ?> getContainer();
}
