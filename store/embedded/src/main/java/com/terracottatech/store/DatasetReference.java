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
package com.terracottatech.store;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.store.builder.EmbeddedDatasetConfiguration;
import com.terracottatech.store.reference.DestroyableReference;
import com.terracottatech.store.wrapper.WrapperDataset;

import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

public class DatasetReference<K extends Comparable<K>> {
  private final Type<K> keyType;
  private final DatasetFactory datasetFactory;
  private final Supplier<Boolean> parentClosed;
  private final DestroyableReference<SovereignDataset<?>> reference = new DestroyableReference<>();

  public DatasetReference(Type<K> keyType, DatasetFactory datasetFactory, Supplier<Boolean> parentClosed) {
    this.keyType = keyType;
    this.datasetFactory = datasetFactory;
    this.parentClosed = parentClosed;
  }

  public Type<K> getKeyType() {
    return keyType;
  }

  public <RK extends Comparable<RK>> Dataset<RK> retrieveAs(String name, Type<RK> type) throws StoreException {
    WrapperDataset<?> dataset = retrieve();
    if (dataset == null) {
      throw new DatasetMissingException("Dataset '" + name + "' not found");
    } else {
      try {
        if (!keyType.equals(type)) {
          throw new DatasetKeyTypeMismatchException("Wrong type for Dataset '" + name
              + "'; requested: " + type + ", found: " + keyType);
        }
        @SuppressWarnings("unchecked")
        Dataset<RK> cast = (Dataset<RK>) dataset;
        return cast;
      } catch (Throwable t) {
        try {
          dataset.close();
        } catch (Throwable tt) {
          t.addSuppressed(tt);
        }
        throw t;
      }
    }
  }

  public WrapperDataset<?> retrieve() throws StoreException {
    SovereignDataset<?> sovereignDataset = reference.acquireValue();

    if (sovereignDataset == null) {
      return null;
    }

    return new WrapperDataset<>(sovereignDataset, ForkJoinPool.commonPool(), reference::releaseValue, parentClosed);
  }

  public void create(String name, EmbeddedDatasetConfiguration configuration) throws StoreException {
    try {
      SovereignDataset<K> sovereignDataset = datasetFactory.create(name, keyType, configuration);
      reference.setValue(sovereignDataset);
    } catch (Throwable t) {
      reference.exception(t);
      throw t;
    }
  }

  public void setValue(SovereignDataset<K> sovereignDataset) {
    reference.setValue(sovereignDataset);
  }

  public void destroy() throws StoreException {
    Optional<SovereignDataset<?>> datasetToDestroy = reference.destroy();

    if (datasetToDestroy.isPresent()) {
      SovereignDataset<?> dataset = datasetToDestroy.get();
      datasetFactory.destroy(dataset);
    }
  }
}
