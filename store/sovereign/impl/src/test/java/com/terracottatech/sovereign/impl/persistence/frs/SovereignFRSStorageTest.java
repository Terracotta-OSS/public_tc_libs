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

import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.persistence.AbstractBaseStorageTest;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 5/19/2015.
 */
public class SovereignFRSStorageTest extends AbstractBaseStorageTest {

  private SovereignFRSStorage storage;
  private PersistenceRoot root;

  @Override
  public SovereignFRSStorage getOrCreateStorage(boolean reopen) throws IOException {
    if (this.storage == null) {
      this.root = new PersistenceRoot(file, reopen? PersistenceRoot.Mode.DONTCARE: PersistenceRoot.Mode.ONLY_IF_NEW);
      this.storage = new SovereignFRSStorage(root, SovereignBufferResource.unlimited());
    }
    return this.storage;
  }

  @Override
  public void shutdownStorage() throws IOException, InterruptedException {
    if (this.storage != null) {
      this.storage.shutdown();
      System.gc();
      this.storage = null;
    }
  }

  @Test
  public void testCreate() throws Exception {

    PersistenceRoot root;
    root = new PersistenceRoot(file, PersistenceRoot.Mode.ONLY_IF_NEW);

    SovereignFRSStorage storage = new SovereignFRSStorage(root, SovereignBufferResource.unlimited());
    storage.startupMetadata().get();
    storage.startupData().get();

    SovereignDataSetConfig<String, FixedTimeReference> config =
        new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class)
            .resourceSize(SovereignDataSetConfig.StorageType.HEAP, 0)
            .versionLimit(2)
            .storage(storage);

    SovereignDataset<String> dataset = new SovereignBuilder<>(config).build();

    CellDefinition<String> foo = CellDefinition.define("foo", Type.STRING);
    dataset.add(SovereignDataset.Durability.IMMEDIATE, "key", foo.newCell("this is my test data"));
    storage.shutdown();

    // Part 2: retrieve the persisted dataset

    PersistenceRoot anotherRoot = new PersistenceRoot(file, PersistenceRoot.Mode.REOPEN);

    SovereignFRSStorage anotherStorage = new SovereignFRSStorage(anotherRoot, SovereignBufferResource.unlimited());
    anotherStorage.startupMetadata().get();
    anotherStorage.startupData().get();

    @SuppressWarnings("unchecked")
    SovereignDataset<String> oldDataset = (SovereignDataset<String>) anotherStorage.getManagedDatasets().iterator().next();

    Assert.assertNotNull(oldDataset.get("key"));
    Assert.assertThat(oldDataset.getTimeReferenceGenerator(), is(instanceOf(FixedTimeReference.Generator.class)));

    anotherStorage.shutdown();
  }

  @Override
  @Test
  public void testMetaCreateEmpty() throws IOException, InterruptedException, RestartStoreException, ExecutionException {
    SovereignFRSStorage storage = getOrCreateStorage(false);
    storage.startupMetadata().get();
    assertThat(storage.getDataSetDescriptions().size(), is(0));
    shutdownStorage();
  }

  @Test
  public void testMetaCreateEmptyAddRetrieve() throws IOException, InterruptedException, RestartStoreException, ExecutionException {
    SovereignFRSStorage storage = getOrCreateStorage(false);
    storage.startupMetadata().get();

    SovereignDatasetImpl<String> ds = makeDataSet(storage);
    storage.registerNewDataset(ds);
    assertThat(storage.getDataSetDescriptions().size(), is(1));
    assertThat(storage.getDataSetDescriptions().iterator().next().getUUID(), is(ds.getUUID()));
    shutdownStorage();
  }

  @Test
  public void testMetaAddShutdownRestart() throws IOException, InterruptedException, RestartStoreException, ExecutionException {
    SovereignFRSStorage storage = getOrCreateStorage(false);
    storage.startupMetadata().get();

    SovereignDatasetImpl<String> ds = makeDataSet(storage);
    shutdownStorage();

    storage = getOrCreateStorage(true);
    storage.startupMetadata().get();

    assertThat(storage.getDataSetDescriptions().size(), is(1));
    assertThat(storage.getDataSetDescriptions().iterator().next().getUUID(), is(ds.getUUID()));

    shutdownStorage();

  }

  @Test
  public void testMetaAddRemoveShutdown() throws IOException, InterruptedException, RestartStoreException, ExecutionException {
    SovereignFRSStorage storage = getOrCreateStorage(false);
    storage.startupMetadata().get();

    SovereignDatasetImpl<String> ds = makeDataSet(storage);
    storage.destroyDataSet(ds.getUUID());
    shutdownStorage();

    storage = getOrCreateStorage(true);
    storage.startupMetadata().get();
    assertThat(storage.getDataSetDescriptions().size(), is(0));

    shutdownStorage();
  }

}
