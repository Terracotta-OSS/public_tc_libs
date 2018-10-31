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

package com.terracottatech.store.manager;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.description.SovereignIndexDescription;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.SovereignDatasetDiskDurability;
import com.terracottatech.sovereign.impl.persistence.AbstractPersistentStorage;
import com.terracottatech.sovereign.impl.persistence.AbstractStorage;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetDiscoveryListener;
import com.terracottatech.store.DatasetFactory;
import com.terracottatech.store.DatasetKeyTypeMismatchException;
import com.terracottatech.store.DatasetMissingException;
import com.terracottatech.store.DatasetReference;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.builder.DiskResource;
import com.terracottatech.store.builder.EmbeddedDatasetConfiguration;
import com.terracottatech.store.builder.EmbeddedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfiguration.ResourceConfiguration;
import com.terracottatech.store.wrapper.WrapperDataset;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class EmbeddedDatasetManager implements DatasetManager, DatasetDiscoveryListener {
  private final DatasetFactory datasetFactory;
  private final ResourceConfiguration resourceConfiguration;
  private final Map<String, DatasetReference<?>> datasets = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean();

  public EmbeddedDatasetManager(DatasetFactory datasetFactory, ResourceConfiguration resourceConfiguration) {
    this.datasetFactory = datasetFactory;
    this.resourceConfiguration = resourceConfiguration;
  }

  @Override
  public <K extends Comparable<K>> void foundExistingDataset(String diskResourceName, SovereignDataset<K> dataset) throws StoreException {
    checkIfClosed();

    String name = dataset.getAlias();
    Type<K> keyType = dataset.getType();

    DatasetReference<K> newReference = new DatasetReference<>(keyType, datasetFactory, closed::get);
    DatasetReference<?> existingReference = datasets.putIfAbsent(name, newReference);

    if (existingReference != null) {
      throw new StoreException("Dataset conflict. Multiple Datasets called: " + name);
    }

    newReference.setValue(dataset);
  }

  @Override
  public DatasetManagerConfiguration getDatasetManagerConfiguration() {
    Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasetInfoMap = new HashMap<>();

    Map<Path, String> pathToDiskResourceNameMap =
        createPathToDiskResourceNameMap(resourceConfiguration.getDiskResources());

    datasets.forEach((datasetName, datasetReference) -> {
      try (WrapperDataset<?> wrapperDataset = datasetReference.retrieve()) {
        // Information needed to reconstruct DatasetConfiguration is scattered across multiple places:
        // SovereignDatasetDescriptionImpl, SovereignDataSetConfig and AbstractPersistentStorage are used to
        // construct the DatasetConfiguration
        SovereignDataset<?> sovereignDataset = wrapperDataset.getBacking();

        SovereignDatasetDescriptionImpl<?, ?> description =
            (SovereignDatasetDescriptionImpl)sovereignDataset.getDescription();

        SovereignDataSetConfig<?, ?> sovereignDataSetConfig = description.getConfig();

        DatasetConfigurationBuilder builder = new EmbeddedDatasetConfigurationBuilder();
        builder = addOffheapResource(builder, sovereignDataSetConfig);
        builder = addDiskResource(builder, sovereignDataSetConfig, pathToDiskResourceNameMap);
        builder = addIndexes(builder, description);
        builder = addDiskDurability(builder, sovereignDataSetConfig);
        builder = addAdvancedConfig(builder, sovereignDataSetConfig);

        datasetInfoMap.put(datasetName,
                           new DatasetManagerConfiguration.DatasetInfo<>(datasetReference.getKeyType(), builder.build()));
      } catch (StoreException e) {
        throw new RuntimeException(e);
      }
    });

    return resourceConfiguration.withDatasetsConfiguration(datasetInfoMap);
  }

  @Override
  public <K extends Comparable<K>> boolean newDataset(String name, Type<K> keyType, DatasetConfiguration configuration) throws StoreException {
    checkIfClosed();

    if (!(configuration instanceof EmbeddedDatasetConfiguration)) {
      throw new IllegalArgumentException("The configuration passed in was not obtained from an embedded DatasetManager: " + configuration.getClass());
    }

    EmbeddedDatasetConfiguration embeddedConfiguration = (EmbeddedDatasetConfiguration) configuration;
    datasetFactory.checkConfiguration(embeddedConfiguration);

    DatasetReference<K> newReference = new DatasetReference<>(keyType, datasetFactory, closed::get);

    while (true) {
      DatasetReference<?> existingReference = datasets.putIfAbsent(name, newReference);

      if (existingReference == null) {
        break;
      }

      try (Dataset<?> existingDataset = existingReference.retrieve()) {
        if (existingDataset != null) {
          if (!existingReference.getKeyType().equals(keyType)) {
            throw new DatasetKeyTypeMismatchException("Dataset \"" + name + "\" already exists with a different key type: requested is " + keyType
                                                      + ", found is " + existingReference.getKeyType());
          }
          return false;
        }

        datasets.remove(name, existingReference);
      }
    }

    try {
      newReference.create(name, embeddedConfiguration);
      return true;
    } catch (Throwable t) {
      datasets.remove(name, newReference);
      throw t;
    }
  }

  @Override
  public <K extends Comparable<K>> Dataset<K> getDataset(String name, Type<K> keyType) throws StoreException {
    checkIfClosed();

    DatasetReference<?> reference = datasets.get(name);
    if (reference == null) {
      throw new DatasetMissingException("Dataset '" + name + "' not found");
    } else {
      return reference.retrieveAs(name, keyType);
    }
  }

  @Override
  public Map<String, Type<?>> listDatasets() {
    Map<String, Type<?>> result = new HashMap<>();

    for (Map.Entry<String, DatasetReference<?>> entry : datasets.entrySet()) {
      String name = entry.getKey();
      DatasetReference<?> datasetReference = entry.getValue();

      result.put(name, datasetReference.getKeyType());
    }

    return Collections.unmodifiableMap(result);
  }

  @Override
  public boolean destroyDataset(String name) throws StoreException {
    checkIfClosed();

    AtomicBoolean destroyed = new AtomicBoolean(false);
    try {
      datasets.compute(name, (datasetName, datasetReference) -> {
        if (datasetReference != null) {
          // Throws if the dataset has another user
          try {
            datasetReference.destroy();
          } catch (StoreException e) {
            throw new DestroyException(e);
          }
          destroyed.set(true);
        }
        return null;
      });
    } catch (DestroyException e) {
      throw e.getWrappedException();
    }

    return destroyed.get();
  }

  @Override
  public void close() {
    boolean closing = closed.compareAndSet(false, true);

    if (closing) {
      datasetFactory.close();
    }
  }

  @Override
  public DatasetConfigurationBuilder datasetConfiguration() {
    return new EmbeddedDatasetConfigurationBuilder();
  }

  private void checkIfClosed() throws StoreException {
    if (closed.get()) {
      throw new StoreException("Attempt to use DatasetManager after close()");
    }
  }

  private static DatasetConfigurationBuilder addOffheapResource(DatasetConfigurationBuilder builder,
                                                                SovereignDataSetConfig<?, ?> sovereignDataSetConfig) {
    return builder.offheap(sovereignDataSetConfig.getOffheapResourceName());
  }

  private static DatasetConfigurationBuilder addDiskResource(DatasetConfigurationBuilder builder,
                                                             SovereignDataSetConfig<?, ?> sovereignDataSetConfig,
                                                             Map<Path, String> reverseMap) {
    AbstractStorage storage = sovereignDataSetConfig.getStorage();
    if (storage.isPersistent()) {
      AbstractPersistentStorage persistentStorage = (AbstractPersistentStorage)storage;
      PersistenceRoot persistenceRoot = persistentStorage.getPersistenceRoot();
      Path dataRoot = persistenceRoot.getDataRoot().getParentFile().toPath().toAbsolutePath();
      if (reverseMap.containsKey(dataRoot)) {
        String diskResourceName = reverseMap.get(dataRoot);
        builder = builder.disk(diskResourceName, persistentStorage.getPersistentStorageType());
      } else {
        throw new StoreRuntimeException("Unable to find disk resource mapping for data root: " + dataRoot);
      }
    }
    return builder;
  }

  private static DatasetConfigurationBuilder addIndexes(DatasetConfigurationBuilder builder,
                                                        SovereignDatasetDescriptionImpl<?, ?> description) {
    List<? extends SovereignIndexDescription<?>> indexDescriptions = description.getIndexDescriptions();
    if (indexDescriptions != null) {
      for (SovereignIndexDescription<?> index : indexDescriptions) {
        builder = builder.index(index.getCellDefinition(), getIndexSettings(index.getIndexSettings()));
      }
    }
    return builder;
  }

  private static DatasetConfigurationBuilder addDiskDurability(DatasetConfigurationBuilder builder,
                                                               SovereignDataSetConfig<?, ?> sovereignDataSetConfig) {
    SovereignDatasetDiskDurability diskDurability = sovereignDataSetConfig.getDiskDurability();
    SovereignDatasetDiskDurability.DurabilityEnum durabilityEnum = diskDurability.getDurabilityEnum();

    switch (durabilityEnum) {
      case TIMED: {
        SovereignDatasetDiskDurability.Timed timed = (SovereignDatasetDiskDurability.Timed)diskDurability;
        builder = builder.durabilityTimed(timed.getNsDuration(), TimeUnit.NANOSECONDS);
        break;
      }
      case NONE: {
        builder = builder.durabilityEventual();
        break;
      }
      case ALWAYS: {
        builder = builder.durabilityEveryMutation();
        break;
      }
    }

    return builder;
  }

  private DatasetConfigurationBuilder addAdvancedConfig(DatasetConfigurationBuilder builder,
                                                        SovereignDataSetConfig<?, ?> sovereignDataSetConfig) {
    return builder.advanced().concurrencyHint(sovereignDataSetConfig.getConcurrency());
  }

  private static Map<Path, String> createPathToDiskResourceNameMap(Map<String, DiskResource> diskResourceMap) {
    Map<Path, String> reverseMap = new HashMap<>();
    diskResourceMap.forEach((key, value) -> reverseMap.put(value.getDataRoot().toAbsolutePath(), key));
    return reverseMap;
  }

  private static IndexSettings getIndexSettings(SovereignIndexSettings sovereignIndexSettings) {
    if (SovereignIndexSettings.BTREE.equals(sovereignIndexSettings)) {
      return IndexSettings.BTREE;
    }
    throw new RuntimeException("Unsupported SovereignIndexSettings: " + sovereignIndexSettings);
  }

  private static final class DestroyException extends StoreRuntimeException {
    private static final long serialVersionUID = 5994747495487493419L;
    private final StoreException wrappedException;
    DestroyException(StoreException wrappedException) {
      super(wrappedException);
      this.wrappedException = wrappedException;
    }

    StoreException getWrappedException() {
      return wrappedException;
    }
  }
}
