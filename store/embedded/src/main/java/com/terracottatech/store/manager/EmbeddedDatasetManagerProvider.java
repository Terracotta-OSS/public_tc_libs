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

import com.terracottatech.store.DatasetFactory;
import com.terracottatech.store.MuxDatasetDiscoveryListener;
import com.terracottatech.store.StorageFactory;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.builder.DiskResource;
import com.terracottatech.store.internal.InternalDatasetManager;
import com.terracottatech.store.management.ManageableDatasetManager;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfiguration;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfiguration.ResourceConfiguration;
import com.terracottatech.store.statistics.StatisticsDatasetManager;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class EmbeddedDatasetManagerProvider extends AbstractDatasetManagerProvider {
  @Override
  public Set<Type> getSupportedTypes() {
    return Collections.singleton(Type.EMBEDDED);
  }

  @Override
  public ClusteredDatasetManagerBuilder clustered(URI uri) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ClusteredDatasetManagerBuilder clustered(Iterable<InetSocketAddress> servers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ClusteredDatasetManagerBuilder secureClustered(URI uri, Path securityRootDirectory) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ClusteredDatasetManagerBuilder secureClustered(Iterable<InetSocketAddress> servers, Path securityRootDirectory) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EmbeddedDatasetManagerBuilder embedded() {
    return new EmbeddedDatasetManagerBuilderImpl(this);
  }

  @Override
  public DatasetManager using(DatasetManagerConfiguration datasetManagerConfiguration,
                              ConfigurationMode configurationMode) throws StoreException {
    if (!(datasetManagerConfiguration instanceof EmbeddedDatasetManagerConfiguration)) {
      throw new IllegalArgumentException("Unexpected configuration type: " + datasetManagerConfiguration.getClass());
    }
    ResourceConfiguration resourceConfiguration =
        ((EmbeddedDatasetManagerConfiguration) datasetManagerConfiguration).getResourceConfiguration();

    Map<String, Long> offheapResources = resourceConfiguration.getOffheapResources();
    Map<String, DiskResource> diskResources = resourceConfiguration.getDiskResources();
    if (offheapResources.isEmpty()) {
      throw new IllegalStateException("Must specify at least one offheap resource");
    }

    Map<String, Long> offheapResourcesCopy = new HashMap<>(offheapResources);
    Map<String, DiskResource> diskResourcesCopy = new HashMap<>(diskResources);

    StorageFactory storageFactory = new StorageFactory(offheapResourcesCopy, diskResourcesCopy);
    DatasetFactory datasetFactory = new DatasetFactory(storageFactory);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, resourceConfiguration);

    storageFactory.startup(new MuxDatasetDiscoveryListener(datasetFactory, datasetManager));

    boolean success = false;
    try {
      ensureDatasets(datasetManager, datasetManagerConfiguration, configurationMode);
      success = true;
    } finally {
      if (!success) {
        datasetManager.close();
      }
    }

    return internal(datasetManager);
  }

  private InternalDatasetManager internal(DatasetManager datasetManager) {
    String instanceIdAndAlias = UUID.randomUUID().toString();
    return new ManageableDatasetManager(instanceIdAndAlias, instanceIdAndAlias, new StatisticsDatasetManager(datasetManager));
  }

}
