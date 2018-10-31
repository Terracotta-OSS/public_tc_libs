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
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfigurationBuilder;

import java.nio.file.Path;

public class EmbeddedDatasetManagerBuilderImpl implements EmbeddedDatasetManagerBuilder {
  private final EmbeddedDatasetManagerProvider provider;
  private final EmbeddedDatasetManagerConfigurationBuilder embeddedDatasetManagerConfigurationBuilder;

  public EmbeddedDatasetManagerBuilderImpl(EmbeddedDatasetManagerProvider provider) {
    this.provider = provider;
    this.embeddedDatasetManagerConfigurationBuilder = new EmbeddedDatasetManagerConfigurationBuilder();
  }

  private EmbeddedDatasetManagerBuilderImpl(EmbeddedDatasetManagerProvider provider,
                                           EmbeddedDatasetManagerConfigurationBuilder embeddedDatasetManagerConfigurationBuilder) {
    this.provider = provider;
    this.embeddedDatasetManagerConfigurationBuilder = embeddedDatasetManagerConfigurationBuilder;
  }

  @Override
  public DatasetManager build() throws StoreException {
    return provider.using(embeddedDatasetManagerConfigurationBuilder.build(), ConfigurationMode.VALIDATE);
  }

  @Override
  public EmbeddedDatasetManagerBuilder offheap(String resource, long unitCount, MemoryUnit memoryUnit) {
    return new EmbeddedDatasetManagerBuilderImpl(provider, embeddedDatasetManagerConfigurationBuilder.offheap(resource,
                                                                                                     unitCount, memoryUnit));
  }

  @Override
  public EmbeddedDatasetManagerBuilder disk(String resource, Path dataRootDirectory, PersistenceMode persistenceMode, FileMode fileMode) {
    return new EmbeddedDatasetManagerBuilderImpl(provider,
                                                 embeddedDatasetManagerConfigurationBuilder.disk(resource, dataRootDirectory, persistenceMode, fileMode));
  }

  @Override
  public EmbeddedDatasetManagerBuilder disk(String resource, Path dataRootDirectory, FileMode fileMode) {
    return new EmbeddedDatasetManagerBuilderImpl(provider,
        embeddedDatasetManagerConfigurationBuilder.disk(resource, dataRootDirectory, fileMode));
  }
}
