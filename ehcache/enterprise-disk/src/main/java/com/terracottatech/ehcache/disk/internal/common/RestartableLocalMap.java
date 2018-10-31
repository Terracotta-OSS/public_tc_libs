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
package com.terracottatech.ehcache.disk.internal.common;

import org.ehcache.spi.persistence.StateHolder;

import com.terracottatech.ehcache.common.frs.RestartableGenericMap;
import com.terracottatech.frs.RestartStore;

import java.nio.ByteBuffer;

/**
 * Extends the restartable map implementation to implement the {@link StateHolder} interface required
 * for local state repositories.
 */
public class RestartableLocalMap<K, V> extends RestartableGenericMap<K, V> implements StateHolder<K, V> {
  public RestartableLocalMap(
      Class<K> keyType,
      Class<V> valueType,
      ByteBuffer identifier,
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore,
      boolean isSynchronous) {
    super(keyType, valueType, identifier, restartStore);
  }
}