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
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.impl.SovereignAllocationResource;
import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.indexing.SimpleIndexDescription;
import com.terracottatech.sovereign.impl.memory.SovereignRuntime;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.sovereign.spi.Space;
import com.terracottatech.sovereign.time.TimeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * @author cschanck
 **/
public abstract class AbstractStorage
  implements SovereignStorage<SovereignDatasetImpl<?>, SovereignDatasetDescriptionImpl<?, ?>> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractStorage.class);
  private final SovereignBufferResource resource;
  private final SovereignAllocationResource allocator;
  protected final ConcurrentHashMap<UUID, SovereignDatasetImpl<?>> datasetMap = new ConcurrentHashMap<>();

  protected ConcurrentMap<UUID, BiConsumer<MetadataKey.Tag<?>, Object>> setMetadataConsumers = new ConcurrentHashMap<>();

  public AbstractStorage(SovereignBufferResource resource) {
    this.resource = resource;
    this.allocator = new SovereignAllocationResource(resource);
  }

  public void addMetaDataConsumer(UUID uuid, BiConsumer<MetadataKey.Tag<?>, Object> consumer) {
    if (!datasetMap.containsKey(uuid)) {
      throw new IllegalStateException("Dataset " + uuid + "does not exist, consumer cannot be added");
    }
    setMetadataConsumers.put(uuid, consumer);
  }

  public void removeMetaDataConsumer(UUID uuid) {
    setMetadataConsumers.remove(uuid);
  }

  public SovereignAllocationResource getAllocator() {
    return allocator;
  }

  @Override
  public SovereignBufferResource getBufferResource() {
    return resource;
  }

  @Override
  public Collection<SovereignDatasetImpl<?>> getManagedDatasets() {
    return new ArrayList<>(datasetMap.values());
  }

  public abstract <K extends Comparable<K>> Space<?, ?> makeSpace(SovereignRuntime<K> runtime);

  @Override
  public SovereignDatasetImpl<?> getDataset(UUID uuid) {
    return datasetMap.get(uuid);
  }

  public  <Z extends TimeReference<Z>> void registerNewDataset(SovereignDatasetImpl<?> ds) {
    datasetMap.put(ds.getUUID(), ds);
  }

  protected void removeDataset(SovereignDatasetImpl<?> ds) {
    datasetMap.remove(ds.getUUID());
  }

  public void setMetadata(MetadataKey<?> key, Object value) {
    BiConsumer<MetadataKey.Tag<?>, Object> consumer = setMetadataConsumers.get(key.getUUID());
    if (consumer != null) {
      consumer.accept(key.getTag(), value);
    }
  }

  public abstract Object retrieveMetadata(MetadataKey<?> key);

  public abstract void removeMetadata(MetadataKey<?> key);

  @Override
  public void shutdown() throws IOException {
    datasetMap.clear();
    allocator.dispose();
  }

  public CachingSequence newCachingSequence() {
    return new CachingSequence(0, 100);
  }

  protected void recreateIndexes(SovereignDatasetImpl<?> dataset, SovereignDatasetDescriptionImpl<?, ?> descr) throws IOException {
    List<SimpleIndexDescription<?>> indexDescriptions = descr.getIndexDescriptions();
    if (!indexDescriptions.isEmpty()) {
      LOG.info("Recreating indexes for dataset: " + descr.getAlias());
      for (SimpleIndexDescription<?> idescr : indexDescriptions) {
        switch (idescr.getState()) {
          case LIVE:
          case LOADING:
            LOG.info("Recreating index for cell: " + idescr.getCellDefinition().name() + " ...");
            try {
              dataset.getIndexing().createIndex(idescr.getCellDefinition(), idescr.getIndexSettings()).call();
            } catch (Exception e) {
              throw new IOException(e);
            }
            break;
          default:
            break;
        }
      }
      LOG.info("Done recreating indexes for dataset: " + descr.getAlias());
    }
  }
}
