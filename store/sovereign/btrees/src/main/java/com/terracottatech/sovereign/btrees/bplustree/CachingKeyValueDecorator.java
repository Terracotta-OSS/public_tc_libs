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
package com.terracottatech.sovereign.btrees.bplustree;

import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecoratorFactory;
import com.terracottatech.sovereign.common.utils.SimpleLongObjectCache;

import java.util.function.LongFunction;

/**
 * @author cschanck
 */
public class CachingKeyValueDecorator<K, V> implements TxDecorator {
  private SimpleLongObjectCache<K> keyCache;
  private SimpleLongObjectCache<V> valueCache;

  public CachingKeyValueDecorator(int ksize, int vsize) {
    keyCache = new SimpleLongObjectCache<>(ksize);
    valueCache = new SimpleLongObjectCache<>(vsize);
  }

  public K getKey(long address, LongFunction<K> keyLookup) {
    K ret = keyCache.get(address);
    if (ret == null) {
      ret = keyLookup.apply(address);
      keyCache.cache(address, ret);
    }
    return ret;
  }

  public V getValue(long address, LongFunction<V> valueLookup) {
    V ret = valueCache.get(address);
    if (ret == null) {
      ret = valueLookup.apply(address);
      valueCache.cache(address, ret);
    }
    return ret;
  }

  public void uncacheKey(long address) {
    keyCache.uncache(address);
  }

  public void uncacheValue(long address) {
    valueCache.uncache(address);
  }

  @Override
  public void preCommitHook() {
    // noop
  }

  @Override
  public void postCommitHook() {
    keyCache.clear();
    valueCache.clear();
  }

  @Override
  public void postBeginHook() {
    // noop
  }

  @Override
  public void preCloseHook() {
    // noop
  }

  @Override
  public void postCloseHook() {
    // noop
  }

  public static <KK, VV> TxDecoratorFactory<CachingKeyValueDecorator<KK, VV>> factory(final int keyCacheSize,
                                                                                      final int valueCacheSize) {
    return new TxDecoratorFactory<CachingKeyValueDecorator<KK, VV>>() {
      @Override
      public CachingKeyValueDecorator<KK, VV> create() {
        return new CachingKeyValueDecorator<>(keyCacheSize, valueCacheSize);
      }
    };
  }
}
