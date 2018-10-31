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
package com.terracottatech.sovereign.impl.persistence.frs;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.AbstractDatasetApiTest;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import static com.terracottatech.sovereign.impl.SovereignDataSetConfig.StorageType.OFFHEAP;

/**
 * @author cschanck
 */
public class SingleVersionFRSDatasetApiTest extends AbstractDatasetApiTest {

  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  private static SovereignFRSStorage storage;

  @Before
  public void createStorage() throws Exception {
    if (storage == null) {
      storage = new SovereignFRSStorage(new PersistenceRoot(tempFolder.newFolder(), PersistenceRoot.Mode
        .CREATE_NEW), SovereignBufferResource.unlimited());
      storage.startupMetadata().get();
      storage.startupData().get();
    }
  }

  @After
  public void destroyStorage() throws Exception {
    if (storage != null) {
      if (storage.isActive()) {
        storage.shutdown();
      }
      storage = null;
    }
  }

  @Override
  protected <K extends Comparable<K>> SovereignDataset<K> createDataset(SovereignDataSetConfig<K, ?> config) {
    return new SovereignBuilder<>(config.resourceSize(OFFHEAP, 32 * RESOURCE_SIZE).concurrency(4).storage(storage)).build();
  }
}
