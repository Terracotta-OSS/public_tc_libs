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

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetKeyTypeMismatchException;
import com.terracottatech.store.DatasetMissingException;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfiguration;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;

/**
 * This is the entry point into the Terracotta store API.
 *
 * It provides methods to create embedded or clustered {@code DatasetManager} which then allow creating, retrieving
 * and destroying {@link Dataset}.
 */
public interface DatasetManager extends AutoCloseable {
  /**
   * Creates a {@link ClusteredDatasetManagerBuilder} for configuring the interaction with a cluster. The passed in URI takes
   * the form {@code terracotta://server1:port,server2:port,...,serverN:port}.
   *
   * @param uri the {@code URI} for the cluster
   * @return a {@code ClusteredDatasetManagerBuilder} that can be used to configure the interaction with the cluster
   */
  static ClusteredDatasetManagerBuilder clustered(URI uri) {
    return DatasetManagerProvider.getDatasetManagerProvider(DatasetManagerProvider.Type.CLUSTERED).clustered(uri);
  }

  /**
   * Creates a {@link ClusteredDatasetManagerBuilder} for configuring the interaction with a cluster.
   *
   * @param servers an {@code Iterable} of {@code InetSocketAddress}es of the servers in the cluster
   * @return a {@code ClusteredDatasetManagerBuilder} that can be used to configure the interaction with the cluster
   */
  static ClusteredDatasetManagerBuilder clustered(Iterable<InetSocketAddress> servers) {
    return DatasetManagerProvider.getDatasetManagerProvider(DatasetManagerProvider.Type.CLUSTERED).clustered(servers);
  }

  /**
   * Creates a ClusteredDatasetManagerBuilder for configuring the interaction with a cluster where the network communication
   * is secured using SSL/TLS. The passed in URI takes the form "terracotta://server1:port,server2:port,...,serverN:port
   *
   * @param uri the URI for the cluster
   * @param securityRootDirectory path to a directory containing security configuration information. The contents of the
   *                              directory must follow the well-defined structure.
   * @return a ClusteredDatasetManagerBuilder that can be used to configure the interaction with the cluster
   */
  static ClusteredDatasetManagerBuilder secureClustered(URI uri, Path securityRootDirectory) {
    return DatasetManagerProvider.getDatasetManagerProvider(DatasetManagerProvider.Type.CLUSTERED).secureClustered(uri, securityRootDirectory);
  }

  /**
   * Creates a ClusteredDatasetManagerBuilder for configuring the interaction with a cluster where the network communication
   * is secured using SSL/TLS. The passed in URI takes the form "terracotta://server1:port,server2:port,...,serverN:port
   *
   * @param servers an {@code Iterable} of {@code InetSocketAddress}es of the servers in the cluster
   * @param securityRootDirectory path to a directory containing security configuration information. The contents of the
   *                              directory must follow the well-defined structure.
   * @return a ClusteredDatasetManagerBuilder that can be used to configure the interaction with the cluster
   */
  static ClusteredDatasetManagerBuilder secureClustered(Iterable<InetSocketAddress> servers, Path securityRootDirectory) {
    return DatasetManagerProvider.getDatasetManagerProvider(DatasetManagerProvider.Type.CLUSTERED).secureClustered(servers, securityRootDirectory);
  }

  /**
   * Creates an {@link EmbeddedDatasetManagerBuilder} for configuring the interaction with embedded {@link Dataset} instances.
   * <p>
   * An embedded {@code Dataset} is hosted in-process, possibly with a local persistence layer.
   *
   * @return an {@code EmbeddedDatasetManagerBuilder} that can be used to configure the interaction with embedded {@code Dataset} instances.
   */
  static EmbeddedDatasetManagerBuilder embedded() {
    return DatasetManagerProvider.getDatasetManagerProvider(DatasetManagerProvider.Type.EMBEDDED).embedded();
  }

  /**
   * Creates a {@link DatasetManager} using the given {@link DatasetManagerConfiguration} and
   * default ConfigurationMode {@link ConfigurationMode#VALIDATE}
   *
   * @param datasetManagerConfiguration {@link DatasetManager} configuration
   * @return a clustered or embedded {@link DatasetManager} instance
   *
   * @throws StoreException if the creation of the {@link DatasetManager} fails or creation/validation fails
   * using ConfigurationMode {@link ConfigurationMode#VALIDATE}
   */
  static DatasetManager using(DatasetManagerConfiguration datasetManagerConfiguration) throws StoreException {
    return using(datasetManagerConfiguration, ConfigurationMode.VALIDATE);
  }

  /**
   * Creates a {@link DatasetManager} using the given {@link DatasetManagerConfiguration} and create/validate
   * {@link Dataset}s as per given {@link ConfigurationMode}
   *
   * @param datasetManagerConfiguration {@link DatasetManager} configuration
   * @param configurationMode configurationMode to use
   * @return a clustered or embedded {@link DatasetManager} instance
   *
   * @throws StoreException if the creation of the {@link DatasetManager} fails or creation/validation fails
   * as per given {@link ConfigurationMode}
   */
  static DatasetManager using(DatasetManagerConfiguration datasetManagerConfiguration, ConfigurationMode configurationMode) throws StoreException {
    DatasetManagerConfiguration.Type datasetManagerConfigurationType = datasetManagerConfiguration.getType();
    switch (datasetManagerConfigurationType) {
      case EMBEDDED: return DatasetManagerProvider.getDatasetManagerProvider(DatasetManagerProvider.Type.EMBEDDED)
        .using(datasetManagerConfiguration, configurationMode);
      case CLUSTERED: return DatasetManagerProvider.getDatasetManagerProvider(DatasetManagerProvider.Type.CLUSTERED)
        .using(datasetManagerConfiguration, configurationMode);
      default: throw new IllegalArgumentException("Unknown datasetManagerConfigurationType: " + datasetManagerConfigurationType);
    }
  }

  /**
   * @return {@link DatasetManagerConfiguration} for this {@link DatasetManager}
   */
  DatasetManagerConfiguration getDatasetManagerConfiguration();

  /**
   * Create a new persistent {@link Dataset} with the given name and key type.
   * <p>
   * Note that if the {@code Dataset} already exists, only the validity of the key type will be checked. The rest of
   * the configuration is not checked for compatibility.
   * <p>
   * A {@code DatasetManager} does not provide any namespacing to the {@link Dataset}s created with it.
   * Multiple {@code DatasetManager} instances connected to the same cluster are equivalent and hence
   * a clustered dataset created by one would be visible to all others.
   *
   * @param <K> the class of the key of the {@code Dataset}
   *
   * @param name the name of the {@code Dataset}
   * @param keyType the type of the key of the {@code Dataset}
   * @param configuration the configuration for the creation of the {@code Dataset}
   * @return {@code true} if the {@code Dataset} was created, {@code false} if it existed already
   *
   * @throws DatasetKeyTypeMismatchException if the named {@code Dataset} exists but with a different key type
   * @throws StoreException if an error occurs creating the {@code Dataset}
   */
  <K extends Comparable<K>> boolean newDataset(String name, Type<K> keyType, DatasetConfiguration configuration) throws StoreException;

  /**
   * Create a new persistent {@link Dataset} with the given name and key type.
   * <p>
   * Note that if the {@code Dataset} already exists, only the validity of the key type will be checked. The rest of
   * the configuration is not checked for compatibility.
   *
   * @param <K> the class of the key of the {@code Dataset}
   *
   * @param name the name of the {@code Dataset}
   * @param keyType the type of the key of the {@code Dataset}
   * @param configurationBuilder the configuration builder for the creation of the {@code Dataset}
   * @return {@code true} if the {@code Dataset} was created, {@code false} if it existed already
   *
   * @throws DatasetKeyTypeMismatchException if the named {@code Dataset} exists but with a different key type
   * @throws StoreException if an error occurs creating the {@code Dataset}
   */
  default <K extends Comparable<K>> boolean newDataset(String name, Type<K> keyType, DatasetConfigurationBuilder configurationBuilder) throws StoreException {
    return newDataset(name, keyType, configurationBuilder.build());
  }

  /**
   * Gets a {@link Dataset} with the specified name and key type.
   *
   * @param name the name of the {@code Dataset} to get
   * @param keyType the type of the key of the {@code Dataset}
   * @param <K> the class of the key of the {@code Dataset}
   *
   * @return the {@code Dataset} matching {@code name} and {@code keyType}
   *
   * @throws DatasetKeyTypeMismatchException if the named {@code Dataset} exists but with a different key type
   * @throws DatasetMissingException if the named {@code Dataset} does not exist
   * @throws StoreException if an error is encountered while retrieving the {@code Dataset}
   */
  <K extends Comparable<K>> Dataset<K> getDataset(String name, Type<K> keyType) throws StoreException;

  /**
   * List all existing {@link Dataset}s known to this {@code DatasetManager}.
   * <p>
   * For an embedded {@code DatasetManager} this means all {@code Dataset} that were created through it. For a clustered
   * {@code DatasetManager} this means all {@code Dataset} that are known to the cluster it connects to.
   *
   * @return a {@link Map} with one entry per {@code Dataset}, each entry having the {@code Dataset} name as key and the {@code Dataset} type as value.
   * @throws StoreException if anything goes wrong while retrieving the list
   */
  Map<String, Type<?>> listDatasets() throws StoreException;

  /**
   * Permanently removes the {@code Dataset} and all the data it contains.
   *
   * @param name the name of the {@code Dataset} to destroy
   *
   * @return {@code true} if the {@code Dataset} was destroyed, {@code false} if it did not exist
   *
   * @throws StoreException if the {@code Dataset} is in use by another client so could not be destroyed.
   */
  boolean destroyDataset(String name) throws StoreException;

  /**
   * Closes the DatasetManager. In a clustered topology, closes connection to remote server.
   * In an embedded topology, disposes all datasets.
   */
  @Override
  void close();

  /**
   * Creates a {@link DatasetConfigurationBuilder} for configuring which resources a {@link Dataset} can use.
   *
   * @return a {@code DatasetConfigurationBuilder} that can be used to configure a {@code Dataset}.
   */
  DatasetConfigurationBuilder datasetConfiguration();
}
