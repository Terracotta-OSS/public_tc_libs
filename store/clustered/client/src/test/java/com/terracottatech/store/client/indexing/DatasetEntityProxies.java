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
package com.terracottatech.store.client.indexing;

import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.indexing.Indexing;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static java.util.Objects.requireNonNull;

/**
 * Provides utility methods to produce proxy instances for {@link DatasetEntity}.
 */
public final class DatasetEntityProxies {

  /**
   * Gets a test {@link DatasetEntity} supporting the {@link DatasetEntity#getIndexing()} method.
   * @param <K> the dataset key type
   * @param id an identifier for this dataset
   * @return a new {@code DatasetEntity} proxy
   */
  @SuppressWarnings("unchecked")
  public static <K extends Comparable<K>> DatasetEntity<K> getIndexingDataset(String id, Indexing indexing) {
    InvocationHandler handler = new IndexingDatasetEntityProxy(id, requireNonNull(indexing, "indexing"));
    return (DatasetEntity<K>) Proxy.newProxyInstance(
        DatasetEntity.class.getClassLoader(), new Class<?>[]{DatasetEntity.class}, handler);
  }

  /**
   * This {@link InvocationHandler} proxies a {@link DatasetEntity} instance providing access to a
   * pre-defined {@link Indexing} instance.
   */
  private static final class IndexingDatasetEntityProxy implements InvocationHandler {

    private final String proxyId;
    private final Indexing indexing;

    private IndexingDatasetEntityProxy(String proxyId, Indexing indexing) {
      this.proxyId = proxyId;
      this.indexing = indexing;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String methodName = method.getName();
      if ("getIndexing".equals(methodName)) {
        return this.indexing;
      } else if ("toString".equals(methodName)) {
        return proxy.getClass().getSimpleName() + "[" + proxyId + "]";
      }
      throw new UnsupportedOperationException("Method '" + method + "' no supported");
    }
  }
}
