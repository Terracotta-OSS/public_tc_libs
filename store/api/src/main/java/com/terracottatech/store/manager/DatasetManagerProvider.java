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

import com.terracottatech.store.StoreException;
import org.slf4j.LoggerFactory;

import com.terracottatech.store.StoreRuntimeException;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Not part of the public API
 */
interface DatasetManagerProvider {
  enum Type {
    CLUSTERED,
    EMBEDDED;

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  Set<Type> getSupportedTypes();

  ClusteredDatasetManagerBuilder clustered(URI uri);

  ClusteredDatasetManagerBuilder clustered(Iterable<InetSocketAddress> servers);

  ClusteredDatasetManagerBuilder secureClustered(URI uri, Path securityRootDirectory);

  ClusteredDatasetManagerBuilder secureClustered(Iterable<InetSocketAddress> servers, Path securityRootDirectory);

  EmbeddedDatasetManagerBuilder embedded();

  DatasetManager using(DatasetManagerConfiguration datasetManagerConfiguration, ConfigurationMode configurationMode) throws StoreException;

  static DatasetManagerProvider getDatasetManagerProvider(DatasetManagerProvider.Type type) {
    List<DatasetManagerProvider> providers = new ArrayList<>();

    ServiceLoader<DatasetManagerProvider> serviceLoader = ServiceLoader.load(DatasetManagerProvider.class, DatasetManagerProvider.class.getClassLoader());
    for (DatasetManagerProvider provider : serviceLoader) {
      try {
        if (provider.getSupportedTypes().contains(type)) {
          providers.add(provider);
        }
      } catch (ServiceConfigurationError e) {
        LoggerFactory.getLogger(DatasetManagerProvider.class)
            .debug("Failed to load a service implementation - if running in OSGi you can probably ignore", e);
      }
    }

    if (providers.isEmpty()) {
      throw new StoreRuntimeException("No " + type + " implementation available. Check that the " + type + " implementation jar is in the classpath.");
    }

    if (providers.size() > 1) {
      throw new StoreRuntimeException("More than one " + type + " implementation found.");
    }

    return providers.get(0);
  }

  static DatasetManagerProvider getDatasetManagerProvider() {
    ServiceLoader<DatasetManagerProvider> serviceLoader = ServiceLoader.load(DatasetManagerProvider.class, DatasetManagerProvider.class.getClassLoader());
    for (DatasetManagerProvider provider : serviceLoader) {
      try {
        return provider;
      } catch (ServiceConfigurationError e) {
        LoggerFactory.getLogger(DatasetManagerProvider.class)
            .debug("Failed to load a service implementation - if running in OSGi you can probably ignore", e);
      }
    }

    throw new StoreRuntimeException("No implementation available. Check that an implementation jar is in the classpath");
  }
}
