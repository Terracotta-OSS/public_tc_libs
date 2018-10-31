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
package com.terracottatech.sovereign.impl.persistence;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.description.SovereignIndexDescription;
import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.memory.MemorySpace;
import com.terracottatech.sovereign.impl.memory.SovereignRuntime;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.sovereign.spi.Space;
import com.terracottatech.sovereign.time.PersistableTimeReferenceGenerator;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * @author cschanck
 **/
public final class StorageTransient extends AbstractStorage {
  private final ConcurrentHashMap<MetadataKey<?>, Object> transientMeta = new ConcurrentHashMap<>();

  public StorageTransient(SovereignBufferResource resource) {
    super(resource);
  }

  @Override
  public Future<Void> startupMetadata() {
    return new Future<Void>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return true;
      }

      @Override
      public Void get() throws InterruptedException, ExecutionException {
        return null;
      }

      @Override
      public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
      }
    };
  }

  @Override
  public Future<Void> startupData() throws IOException {
    return startupMetadata(); // lazy reuse.
  }

  @Override
  public Collection<SovereignIndexDescription<?>> getIndexes(UUID uuid) {
    return Collections.emptyList();
  }

  @Override
  public void shutdown() throws IOException {
    for (SovereignDatasetImpl<?> ds : getManagedDatasets()) {
      ds.flush();
      ds.dispose();
      removeDataset(ds);
    }
    super.shutdown();
    transientMeta.clear();
  }

  @Override
  public Collection<SovereignDatasetDescriptionImpl<?, ?>> getDataSetDescriptions() {
    ArrayList<SovereignDatasetDescriptionImpl<?, ?>> ret = new ArrayList<>();
    for (SovereignDatasetImpl<?> ds : getManagedDatasets()) {
      ret.add(ds.getDescription());
    }
    return ret;
  }

  @Override
  public void destroyDataSet(UUID uuid) {
    SovereignDatasetImpl<?> ds = getDataset(uuid);
    if (ds != null) {
      ds.dispose();
      removeDataset(ds);
      removeMetadata(MetadataKey.Tag.DATASET_DESCR.keyFor(uuid));
    }
  }

  @Override
  public boolean isPersistent() {
    return false;
  }

  @Override
  public <K extends Comparable<K>> Space<?, ?> makeSpace(SovereignRuntime<K> runtime) {
    return new MemorySpace(runtime);
  }

  @Override
  public  <Z extends TimeReference<Z>> void registerNewDataset(SovereignDatasetImpl<?> ds) {
    super.registerNewDataset(ds);

    ds.getRuntime().getSequence().setCallback(cachingSequence -> {
      BiConsumer<MetadataKey.Tag<?>, Object> consumer = setMetadataConsumers.get(ds.getUUID());
      if (consumer != null) {
        consumer.accept(MetadataKey.Tag.CACHING_SEQUENCE.keyFor(ds.getUUID()).getTag(), ds.getSequence());
      }
    });

    ds.getRuntime().getSchema().getBackend().setCallback(datasetSchemaBackend -> {
      BiConsumer<MetadataKey.Tag<?>, Object> consumer = setMetadataConsumers.get(ds.getUUID());
      if (consumer != null) {
        consumer.accept(MetadataKey.Tag.SCHEMA.keyFor(ds.getUUID()).getTag(),
                        ds.getRuntime().getSchema().getBackend().getPersistable());
      }
    });
    /*
     * Set no-op persistence callback for PersistableTimeReferenceGenerator.
     */
    final TimeReferenceGenerator<?> timeReferenceGenerator = ds.getTimeReferenceGenerator();
    if (timeReferenceGenerator instanceof PersistableTimeReferenceGenerator) {
      ((PersistableTimeReferenceGenerator<?>) timeReferenceGenerator).setPersistenceCallback(() -> null);
    }
  }

  @Override
  public void setMetadata(MetadataKey<?> key, Object value) {
    transientMeta.put(key, value);
    super.setMetadata(key, value);
  }

  @Override
  public Object retrieveMetadata(MetadataKey<?> key) {
    return transientMeta.get(key);
  }

  @Override
  public void removeMetadata(MetadataKey<?> key) {
    transientMeta.remove(key);
  }

}
