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
package com.terracottatech.sovereign.impl.persistence.base;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.description.SovereignIndexDescription;
import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.SovereignDatasetDiskDurability;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaBackend;
import com.terracottatech.sovereign.impl.dataset.metadata.PersistableSchemaList;
import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.persistence.AbstractPersistentStorage;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.sovereign.time.PersistableTimeReferenceGenerator;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import static com.terracottatech.sovereign.exceptions.SovereignExtinctionException.ExtinctionType.PERSISTENCE_FLUSH_FAILURE;
import static com.terracottatech.sovereign.impl.persistence.base.MetadataKey.Tag.DATASET_DESCR;

/**
 * @author cschanck
 **/
public abstract class AbstractRestartabilityBasedStorage extends AbstractPersistentStorage {
  public static final int SOV_COMPACTION_RUN_INTERVAL_SECONDS=180;
  public static final float SOV_COMPACTION_TRIGGER_THRESHOLD_PERCENTAGE = 0.5f;
  public static final float SOV_COMPACTION_PERCENTAGE_AMOUNT = 0.25f;

  private State state;

  public enum State {
    SHUTDOWN,
    META_ONLY,
    META_AND_DATA,
    DEAD
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractRestartabilityBasedStorage.class);
  private static final ByteBuffer META_ID = ByteBuffer.wrap(new byte[4]);
  private final PersistenceRoot root;
  private MetadataMap metaMap;
  private RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> metaObjectManager;
  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> metaStore;
  private RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> dataObjectManager;
  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> datasetStore;
  private final int compactionRunIntervalSeconds;
  private final float compactionTriggerThresholdPercentage;
  private final float compactionPercentageAmount;

  public AbstractRestartabilityBasedStorage(PersistenceRoot root,
                                            SovereignBufferResource resource,
                                            int compactionRunIntervalSeconds,
                                            float compactionTriggerThresholdPercentage,
                                            float compactionPercentageAmount) {
    super(resource);
    this.compactionRunIntervalSeconds = Math.max(15, compactionRunIntervalSeconds);
    this.compactionTriggerThresholdPercentage = compactionTriggerThresholdPercentage;
    this.compactionPercentageAmount = compactionPercentageAmount;
    this.root = root;
    this.state = State.SHUTDOWN;
  }

  public abstract boolean isFRSHybrid();

  @Override
  public SovereignDatasetDiskDurability getDefaultDiskDurability() {
    return SovereignDatasetDiskDurability.timed(1, TimeUnit.SECONDS);
  }

  @Override
  public boolean isPersistent() {
    return true;
  }

  @Override
  public PersistenceRoot getPersistenceRoot() {
    return root;
  }

  public RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> getMetaObjectManager() {
    return metaObjectManager;
  }

  public RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> getMetaStore() {
    return metaStore;
  }

  public RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> getDataObjectManager() {
    return dataObjectManager;
  }

  public RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> getDatasetStore() {
    return datasetStore;
  }

  @Override
  public void removeMetadata(MetadataKey<?> key) {
    key.deleteInMap(metaMap);
  }

  @Override
  public Object retrieveMetadata(MetadataKey<?> key) {
    return key.getInMap(metaMap);
  }

  @Override
  public void setMetadata(MetadataKey<?> key, Object value) {
    key.putInMap(metaMap, value);
    super.setMetadata(key, value);
  }

  @Override
  public Collection<SovereignDatasetDescriptionImpl<?, ?>> getDataSetDescriptions() {
    ArrayList<SovereignDatasetDescriptionImpl<?, ?>> arr = new ArrayList<>();
    for (Map.Entry<MetadataKey<?>, Object> ent : metaMap.entrySet()) {
      if (ent.getKey().getTag() == DATASET_DESCR) {
        arr.add(DATASET_DESCR.cast(ent.getValue()));
      }
    }
    return arr;
  }

  private RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> createObjectManager() {
    return new RegisterableObjectManager<>();
  }

  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createStore(File root,
                                                                       ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager) throws RestartStoreException, IOException {
    final Properties properties = new Properties();
    properties.put(FrsProperty.COMPACTOR_POLICY.shortName(), "SizeBasedCompactionPolicy");
    properties.put(FrsProperty.COMPACTOR_RUN_INTERVAL.shortName(),
                   Integer.toString(compactionRunIntervalSeconds));
    properties.put(FrsProperty.COMPACTOR_SIZEBASED_AMOUNT.shortName(),
                   Float.toString(compactionPercentageAmount));
    properties.put(FrsProperty.COMPACTOR_SIZEBASED_THRESHOLD.shortName(),
                   Float.toString(compactionTriggerThresholdPercentage));
    if (isFRSHybrid()) {
      properties.put(FrsProperty.IO_NIO_ACCESS_METHOD.shortName(), "MAPPED");
      properties.put(FrsProperty.IO_RANDOM_ACCESS.shortName(), "true");
      properties.put(FrsProperty.IO_NIO_RANDOM_ACCESS_MEMORY_SIZE.shortName(), Long.toString(256L * 1024 * 1024));
    } else {
      // this is bogus, in that mapped implies random, even though it shouldn't.
      properties.put(FrsProperty.IO_NIO_ACCESS_METHOD.shortName(), "STREAM");
      properties.put(FrsProperty.IO_RANDOM_ACCESS.shortName(), "false");
    }

    return RestartStoreFactory.createStore(objectManager, root, properties);
  }

  private void verifyState(State... states) {
    for (State s : states) {
      if (s == this.state) {
        return;
      }
    }
    throw new IllegalStateException("Current state: " + this.state);
  }

  @Override
  public synchronized Future<Void> startupMetadata() throws IOException {
    verifyState(State.SHUTDOWN);
    final Future<Void> meta = metaStartup();
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
        return meta.isDone();
      }

      @Override
      public Void get() throws InterruptedException, ExecutionException {
        Void ret = meta.get();
        AbstractRestartabilityBasedStorage.this.state = State.META_ONLY;
        return ret;
      }

      @Override
      public Void get(long timeout,
                      TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
      }
    };
  }

  @Override
  public Collection<SovereignIndexDescription<?>> getIndexes(UUID uuid) {
    ArrayList<SovereignIndexDescription<?>> ret = new ArrayList<>();
    MetadataKey<SovereignDatasetDescriptionImpl<?, ?>> k = DATASET_DESCR.keyFor(uuid);
    SovereignDatasetDescriptionImpl<?, ?> descr = DATASET_DESCR.cast(k.getInMap(metaMap));
    if (descr == null) {
      throw new IllegalStateException();
    }
    for (SovereignIndexDescription<?> idx : descr.getIndexDescriptions()) {
      ret.add(idx);
    }
    return ret;
  }

  @Override
  public synchronized void shutdown() throws IOException {
    // need to do this...
    wasShutdown = true;
    super.killSyncher();
    if (state == State.DEAD) {
      return;
    }
    if (state == State.SHUTDOWN) {
      this.state = State.DEAD;
    }
    if (state == State.META_AND_DATA) {
      dataShutdown();
    }
    metaShutdown();
    this.state = State.DEAD;
    super.shutdown();
  }

  private void dataShutdown() throws IOException {
    try {
      for (SovereignDatasetImpl<?> ds : getManagedDatasets()) {
        ds.flush();
        ds.dispose();
      }
      if (datasetStore != null) {
        datasetStore.shutdown();
        datasetStore = null;
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void explicitFSynch() {
    try {
      datasetStore.beginTransaction(true).commit();
    } catch (TransactionException e) {
      throw PERSISTENCE_FLUSH_FAILURE.exception(e);
    }
  }

  @Override
  public synchronized <Z extends TimeReference<Z>> Future<Void> startupData() throws IOException {
    verifyState(State.META_ONLY);

    try {
      this.datasetStore = createDataStore();
      LOG.info("Restartable startup: " + root);
      final List<SovereignRestartableBroker<?>> brokers = new LinkedList<>();
      List<UUID> pendingDeletes=new LinkedList<>();
      for (Map.Entry<MetadataKey<?>, Object> ent : metaMap.entrySet()) {
        final MetadataKey<?> mkey = ent.getKey();
        if (mkey.getTag() == DATASET_DESCR) {
          MetadataKey.Facade<Z> facade = new MetadataKey.Facade<>(mkey.getUUID(), metaMap);
          repairDescriptor(facade);
          final CachingSequence seq = facade.sequence();
          final PersistableTimeReferenceGenerator<Z> timeref = facade.timeGenerator();
          final SovereignDatasetDescriptionImpl<?, Z> descr = facade.description();
          final PersistableSchemaList schemaList = facade.schemaBackend();
          final boolean pendingDelete = facade.isPendingDelete();
          if(pendingDelete) {
            pendingDeletes.add(mkey.getUUID());
          }
          if (timeref != null) {
            descr.getConfig().timeReferenceGenerator(timeref);
            timeref.setPersistenceCallback(() -> {
              facade.setTimegen(timeref);
              return null;
            });
          } else {
            descr.getConfig().setDefaultTimeReferenceGenerator();
          }

          SovereignDatasetImpl<?> ds = new SovereignDatasetImpl<>(descr.getConfig(),
                                                               descr.getUUID(),
                                                               seq);
          ds.getRuntime().getSchema().initialize(schemaList);
          final DatasetSchemaBackend realSchema = ds.getRuntime().getSchema().getBackend();
          realSchema.setCallback((r) -> {
            facade.setSchemaBackend(realSchema.getPersistable());
            BiConsumer<MetadataKey.Tag<?>, Object> consumer = setMetadataConsumers.get(ds.getUUID());
            if (consumer != null) {
              consumer.accept(MetadataKey.Tag.DATASET_DESCR.keyFor(ds.getUUID()).getTag(),
                              ds.getRuntime().getSchema().getBackend().getPersistable());
            }
          });

          SovereignRestartableBroker<?> broker = makeBroker(ds);

          super.registerNewDataset(ds);

          ds.getContainer().setBroker(broker);

          seq.setCallback(r -> {
            facade.setSequence(seq);
            BiConsumer<MetadataKey.Tag<?>, Object> consumer = setMetadataConsumers.get(ds.getUUID());
            if (consumer != null) {
              consumer.accept(MetadataKey.Tag.CACHING_SEQUENCE.keyFor(ds.getUUID()).getTag(), ds.getRuntime().getSequence());
            }
          });

          LOG.info("Registered restartable dataset: " + ds.getAlias());
          dataObjectManager.registerObject(broker);
          brokers.add(broker);
        }
      }
      final Future<Void> data = datasetStore.startup();
      return new Future<Void>() {
        boolean done = false;

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
          return done;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Void get() throws InterruptedException, ExecutionException {
          Void ret = data.get();
          brokers.forEach(SovereignRestartableBroker::finishRestart);
          AbstractRestartabilityBasedStorage.this.state = State.META_AND_DATA;
          done = true;
          // process pending deletes
          for (UUID uuid : pendingDeletes) {
            try {
              LOG.info("Processing pending delete for dataset: " + uuid);
              destroyDataSet(uuid);
            } catch (IOException e) {
              throw new ExecutionException(e);
            }
          }
          for (SovereignDatasetImpl<?> dataset : getManagedDatasets()) {
            MetadataKey key = MetadataKey.Tag.DATASET_DESCR.keyFor(dataset.getUUID());
            SovereignDatasetDescriptionImpl<?, ?> idescr = (SovereignDatasetDescriptionImpl<?, ?>) metaMap.get(key);
            try {
              recreateIndexes(dataset,idescr);
            } catch (IOException e) {
              // TODO, error case here
              throw new ExecutionException(e);
            }
          }

          return ret;
        }

        @Override
        public Void get(long timeout,
                        TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
          return get();
        }
      };
    } catch (InterruptedException | RestartStoreException e) {
      throw new IOException(e);
    }
  }

  // Fix any corruption caused by TDB-2576
  private void repairDescriptor(MetadataKey.Facade<?> facade) {
    UUID uuid = facade.getUUID();
    SovereignDatasetDescriptionImpl<?, ?> description = facade.description();
    UUID uuidFromDescription = description.getUUID();
    if (!uuid.equals(uuidFromDescription)) {
      LOG.info("Repaired UUID in descriptor. Dataset UUID: " + uuid + " descriptor UUID: " + uuidFromDescription);
      SovereignDatasetDescriptionImpl<?, ?> normalizedDescription = SovereignDatasetDescriptionImpl.normalizeUUID(description, uuid);
      facade.setDescription(normalizedDescription);
    }
  }

  // currently an unused method, but may come back again.
  private void reindexPrimaryKeys() {
    for (SovereignDatasetImpl<?> ds : getManagedDatasets()) {
      try (ContextImpl c = ds.getContainer().start(false)) {
        PersistentMemoryLocator loc = ds.getContainer().first(c);
        while (loc.isValid()) {
          ds.getPrimary().reinstall(ds.getContainer().get(loc).getKey(), loc);
          loc = loc.next();
        }
      }
    }
  }

  private synchronized Future<Void> metaStartup() throws IOException {
    try {
      metaStore = createMetaStore();
      metaMap = new MetadataMap(this, META_ID, metaStore);
      metaObjectManager.registerObject(metaMap);
      LOG.info("Meta startup for: " + root.getRoot());
      return metaStore.startup();
    } catch (InterruptedException | RestartStoreException e) {
      throw new IOException(e);
    }
  }

  private synchronized void metaShutdown() throws IOException {
    try {
      if (metaStore != null) {
        metaStore.shutdown();
        // Release references through which a DirectByteBuffer is held
        metaStore = null;
        metaMap = null;
        metaObjectManager = null;
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createMetaStore() throws IOException, RestartStoreException {
    metaObjectManager = createObjectManager();
    return createStore(root.getMetaRoot(), metaObjectManager);
  }

  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createDataStore() throws IOException, RestartStoreException {
    dataObjectManager = createObjectManager();
    return createStore(root.getDataRoot(), dataObjectManager);
  }

  @Override
  public synchronized  <Z extends TimeReference<Z>>  void registerNewDataset(SovereignDatasetImpl<?> impl) {
    if (!DATASET_DESCR.keyFor(impl.getUUID()).keyInMap(metaMap)) {
      if (impl.records().count() != 0) {
        throw new IllegalStateException();
      }

      SovereignDatasetDescriptionImpl<? extends Comparable<?>, ?> dsd = impl.getDescription();

      MetadataKey.Facade<Z> facade = new MetadataKey.Facade<>(impl.getUUID(), metaMap);
      facade.setSchemaBackend(impl.getRuntime().getSchema().getBackend().getPersistable());
      facade.setSequence(impl.getSequence());
      facade.setDescription(dsd);

      impl.getRuntime().getSchema().getBackend().setCallback((r) -> {
        PersistableSchemaList pers = r.getPersistable();
        facade.setSchemaBackend(pers);
        BiConsumer<MetadataKey.Tag<?>, Object> consumer = setMetadataConsumers.get(impl.getUUID());
        if (consumer != null) {
          consumer.accept(MetadataKey.Tag.SCHEMA.keyFor(impl.getUUID()).getTag(), pers);
        }
      });

      impl.getSequence().setCallback(r -> {
        facade.setSequence(impl.getSequence());
        BiConsumer<MetadataKey.Tag<?>, Object> consumer = setMetadataConsumers.get(impl.getUUID());
        if (consumer != null) {
          consumer.accept(MetadataKey.Tag.CACHING_SEQUENCE.keyFor(impl.getUUID()).getTag(), impl.getSequence());
        }
      });

      final TimeReferenceGenerator<?> timeReferenceGenerator = impl.getTimeReferenceGenerator();
      if (timeReferenceGenerator instanceof PersistableTimeReferenceGenerator) {
        @SuppressWarnings("unchecked")
        PersistableTimeReferenceGenerator<Z> pgen = (PersistableTimeReferenceGenerator) timeReferenceGenerator;
        facade.setTimegen(pgen);
        pgen.setPersistenceCallback(() -> {
          facade.setTimegen(pgen);
          return null;
        });
      }

      if (dataObjectManager != null) {
        SovereignRestartableBroker<?> broker = makeBroker(impl);
        LOG.info("Registered new persistent dataset: " + impl.getUUID());
        dataObjectManager.registerObject(broker);
      }
      super.registerNewDataset(impl);
    }
  }

  protected abstract SovereignRestartableBroker<?> makeBroker(SovereignDatasetImpl<?> impl);

  @Override
  public void destroyDataSet(UUID uuid) throws IOException {
    if (state == State.DEAD) {
      return;
    }
    destroyDataSet(uuid, false);
  }

  public synchronized void destroyDataSet(UUID uuid, boolean pendingOnly) throws IOException {
    if (DATASET_DESCR.keyFor(uuid).keyInMap(metaMap)) {
      SovereignDatasetImpl<?> ds = getDataset(uuid);
      ds.dispose();
      MetadataKey.Tag.PENDING_DELETE.keyFor(ds.getUUID()).putInMap(metaMap, Boolean.TRUE);
      if (!pendingOnly) {
        if (dataObjectManager != null) {
          try {
            datasetStore.beginAutoCommitTransaction(true).delete(SovereignRestartableBroker.uuidToBuffer(ds.getUUID()));
          } catch (TransactionException e) {
            throw new IOException(e);
          }
          dataObjectManager.unregisterStripe(SovereignRestartableBroker.uuidToBuffer(ds.getUUID()));
        }
        for (MetadataKey.Tag<?> tt : MetadataKey.Tag.values()) {
          tt.keyFor(uuid).deleteInMap(metaMap);
        }
      }
      removeDataset(ds);
    } else {
      throw new IllegalArgumentException();
    }
  }

  public synchronized boolean isActive() {
    return this.state == State.META_ONLY || this.state == State.META_AND_DATA;
  }

}
