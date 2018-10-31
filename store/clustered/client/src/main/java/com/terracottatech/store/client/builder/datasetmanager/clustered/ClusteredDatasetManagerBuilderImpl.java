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
package com.terracottatech.store.client.builder.datasetmanager.clustered;

import com.terracottatech.store.StoreException;
import com.terracottatech.store.manager.ClusteredDatasetManagerBuilder;
import com.terracottatech.store.manager.ClusteredDatasetManagerProvider;
import com.terracottatech.store.manager.ConfigurationMode;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfigurationBuilder;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ClusteredDatasetManagerBuilderImpl implements ClusteredDatasetManagerBuilder {

  private final ClusteredDatasetManagerConfigurationBuilder clusteredDatasetManagerConfigurationBuilder;
  private final ClusteredDatasetManagerProvider provider;

  public ClusteredDatasetManagerBuilderImpl(ClusteredDatasetManagerProvider provider, URI clusterUri) {
    this(provider, clusterUri, null);
  }

  public ClusteredDatasetManagerBuilderImpl(ClusteredDatasetManagerProvider provider, URI clusterUri, Path securityRootDirectory) {
    this(provider, new ClusteredDatasetManagerConfigurationBuilder(clusterUri, securityRootDirectory));
  }

  public ClusteredDatasetManagerBuilderImpl(ClusteredDatasetManagerProvider provider, Iterable<InetSocketAddress> servers) {
    this(provider, new ClusteredDatasetManagerConfigurationBuilder(servers, null));
  }

  public ClusteredDatasetManagerBuilderImpl(ClusteredDatasetManagerProvider provider, Iterable<InetSocketAddress> servers, Path securityRootDirectory) {
    this(provider, new ClusteredDatasetManagerConfigurationBuilder(servers, securityRootDirectory));
  }

  private ClusteredDatasetManagerBuilderImpl(ClusteredDatasetManagerProvider provider, ClusteredDatasetManagerConfigurationBuilder clusteredDatasetManagerConfigurationBuilder) {
    this.provider = provider;
    this.clusteredDatasetManagerConfigurationBuilder = clusteredDatasetManagerConfigurationBuilder;
  }

  @Override
  public DatasetManager build() throws StoreException {
    return provider.using(clusteredDatasetManagerConfigurationBuilder.build(), ConfigurationMode.VALIDATE);
  }

  @Override
  public ClusteredDatasetManagerBuilder withConnectionTimeout(long timeout, TimeUnit unit) {
    return new ClusteredDatasetManagerBuilderImpl(provider,
                                                  clusteredDatasetManagerConfigurationBuilder.withConnectionTimeout(timeout, unit));
  }

  @Override
  public ClusteredDatasetManagerBuilder withReconnectTimeout(long timeout, TimeUnit unit) {
    return new ClusteredDatasetManagerBuilderImpl(provider, clusteredDatasetManagerConfigurationBuilder.withReconnectTimeout(timeout, unit));
  }

  @Override
  public ClusteredDatasetManagerBuilder withClientAlias(String alias) {
    return new ClusteredDatasetManagerBuilderImpl(provider, clusteredDatasetManagerConfigurationBuilder.withClientAlias(alias));
  }

  @Override
  public ClusteredDatasetManagerBuilder withClientTags(Set<String> tags) {
    return new ClusteredDatasetManagerBuilderImpl(provider, clusteredDatasetManagerConfigurationBuilder.withClientTags(tags));
  }
}
