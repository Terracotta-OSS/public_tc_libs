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
package com.terracottatech.sovereign.impl.model;

import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.spi.store.PrimaryMap;

/**
 * Created by cschanck on 8/26/2015.
 */
public interface SovereignPrimaryMap<K extends Comparable<K>> extends PrimaryMap<K, ContextImpl,
  PersistentMemoryLocator>, SovereignIndexMap<K, K> {

  @Override
  default PersistentMemoryLocator put(K recordKey, ContextImpl context, K key, PersistentMemoryLocator pointer) {
    return put(context, recordKey, pointer);
  }

  @Override
  default boolean remove(K recordKey, ContextImpl context, K key, PersistentMemoryLocator pointer) {
    return remove(context, recordKey, pointer);
  }
}
