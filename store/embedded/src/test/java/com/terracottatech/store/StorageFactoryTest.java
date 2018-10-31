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
package com.terracottatech.store;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.impl.persistence.StorageTransient;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.builder.DiskResource;
import com.terracottatech.store.builder.EmbeddedDatasetConfiguration;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.terracottatech.store.configuration.PersistentStorageEngine.FRS;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.NEW;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.REOPEN;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StorageFactoryTest {
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void discoversExistingDatasets() throws Exception {
    Path dataRoot = testFolder.newFolder("storage-factory-test").toPath();

    createDatasets(dataRoot, Arrays.asList("abc"), Arrays.asList("offheap"));

    Map<String, Long> offheapResourceSizes = Collections.singletonMap("offheap", 10L * 1024L * 1024L);
    Map<String, DiskResource> diskResources = Collections.singletonMap("disk", new DiskResource(dataRoot, FRS, REOPEN));
    try (StorageFactory storageFactory = new StorageFactory(offheapResourceSizes, diskResources)) {

      TestDatasetDiscoveryListener listener = new TestDatasetDiscoveryListener();
      storageFactory.startup(listener);

      List<String> aliases = listener.getAliases();
      assertEquals(1, aliases.size());
      assertTrue(aliases.contains("abc"));
    }
  }

  @Test(expected = StoreRuntimeException.class)
  public void discoversMultipleDatasetsFails() throws Exception {
    Path dataRoot = testFolder.newFolder("storage-factory-test").toPath();

    createDatasets(dataRoot, Arrays.asList("abc", "def"), Arrays.asList("offheap1", "offheap2"));

    Map<String, Long> offheapResourceSizes = new HashMap<>();
    offheapResourceSizes.put("offheap1", 10L * 1024L * 1024L);
    offheapResourceSizes.put("offheap2", 10L * 1024L * 1024L);
    Map<String, DiskResource> diskResources = Collections.singletonMap("disk", new DiskResource(dataRoot, FRS, REOPEN));
    try (StorageFactory storageFactory = new StorageFactory(offheapResourceSizes, diskResources)) {
      TestDatasetDiscoveryListener listener = new TestDatasetDiscoveryListener();
      storageFactory.startup(listener);
    }
  }

  @Test
  public void createNewStorage() throws Exception {
    Path dataRoot = testFolder.newFolder("storage-factory-test").toPath();

    Map<String, Long> offheapResourceSizes = new HashMap<>();
    offheapResourceSizes.put("offheap1", 10L * 1024L * 1024L);
    offheapResourceSizes.put("offheap2", 10L * 1024L * 1024L);
    Map<String, DiskResource> diskResources = Collections.singletonMap("disk", new DiskResource(dataRoot, FRS, NEW));
    StorageFactory storageFactory = new StorageFactory(offheapResourceSizes, diskResources);

    storageFactory.startup(null);

    EmbeddedDatasetConfiguration configuration = new EmbeddedDatasetConfiguration("offheap1", "disk", emptyMap());
    SovereignStorage<?, ?> storage = storageFactory.getStorage(configuration);
    assertTrue(storage instanceof SovereignFRSStorage);

    createDatasets(storage, Arrays.asList("newABC", "newDEF"), Arrays.asList("offheap1", "offheap2"));

    @SuppressWarnings("unchecked")
    Collection<SovereignDataset<?>> datasets = (Collection<SovereignDataset<?>>) storage.getManagedDatasets();
    List<String> aliases = datasets.stream().map(SovereignDataset::getAlias).collect(Collectors.toCollection(ArrayList::new));
    assertEquals(2, aliases.size());
    assertTrue(aliases.contains("newABC"));
    assertTrue(aliases.contains("newDEF"));

    SovereignStorage<?, ?> storage1 = storageFactory.getStorage(configuration);
    assertEquals(storage, storage1);

    try {
      EmbeddedDatasetConfiguration configuration2 = new EmbeddedDatasetConfiguration("offheap2", "disk", emptyMap());
      storageFactory.getStorage(configuration2);
      fail("Expected exception but didn't get one");
    } catch (StoreRuntimeException e) {
      // Expected exception
    }

    EmbeddedDatasetConfiguration configuration3 = new EmbeddedDatasetConfiguration("offheap1", null, emptyMap());
    SovereignStorage<?, ?> storage3 = storageFactory.getStorage(configuration3);
    assertTrue(storage3 instanceof StorageTransient);

    EmbeddedDatasetConfiguration configuration4 = new EmbeddedDatasetConfiguration("offheap2", null, emptyMap());
    SovereignStorage<?, ?> storage4 = storageFactory.getStorage(configuration4);
    assertTrue(storage4 instanceof StorageTransient);

    assertNotEquals(storage3, storage);
    assertNotEquals(storage4, storage);
    assertNotEquals(storage3, storage4);

    createDatasets(storage3, Arrays.asList("333"), Arrays.asList("offheap1"));
    createDatasets(storage4, Arrays.asList("444"), Arrays.asList("offheap2"));

    assertNotEquals(0, storage.getManagedDatasets().size());
    assertNotEquals(0, storage3.getManagedDatasets().size());
    assertNotEquals(0, storage4.getManagedDatasets().size());
    storageFactory.close();
    assertEquals(0, storage.getManagedDatasets().size());
    assertEquals(0, storage3.getManagedDatasets().size());
    assertEquals(0, storage4.getManagedDatasets().size());
  }

  @Test
  public void testCachedStorageNotReturnedIfShutdown() throws Exception {
    Path dataRoot = testFolder.newFolder("storage-factory-test").toPath();

    Map<String, Long> offheapResourceSizes = new HashMap<>();
    offheapResourceSizes.put("offheap1", 10L * 1024L * 1024L);
    offheapResourceSizes.put("offheap2", 10L * 1024L * 1024L);
    Map<String, DiskResource> diskResources = Collections.singletonMap("disk", new DiskResource(dataRoot, FRS, REOPEN_OR_NEW));
    try (StorageFactory storageFactory = new StorageFactory(offheapResourceSizes, diskResources)) {
      storageFactory.startup(null);

      EmbeddedDatasetConfiguration configuration = new EmbeddedDatasetConfiguration("offheap1", "disk", emptyMap());
      SovereignStorage<?, ?> storage = storageFactory.getStorage(configuration);

      assertTrue(storage instanceof SovereignFRSStorage);
      storage.shutdown();

      SovereignStorage<?, ?> storage1 = storageFactory.getStorage(configuration);

      assertNotEquals(storage, storage1);
      storage1.shutdown();
    }
  }

  private void createDatasets(Path dataRoot, List<String> names, List<String> offheapResources) throws Exception {
    PersistenceRoot root = new PersistenceRoot(dataRoot.toFile(), PersistenceRoot.Mode.CREATE_NEW);
    SovereignBufferResource resource = SovereignBufferResource.unlimited();

    SovereignStorage<?, ?> storage = new SovereignFRSStorage(root, resource);
    storage.startupMetadata().get();
    storage.startupData().get();

    createDatasets(storage, names, offheapResources);

    storage.shutdown();
  }

  private void createDatasets(SovereignStorage<?, ?> storage, List<String> names, List<String> offheapResources) throws Exception {
    assert(names.size() == offheapResources.size());

    IntStream.range(0, names.size())
        .mapToObj(i -> Tuple.of(names.get(i), offheapResources.get(i)))
        .forEach(tuple -> {
          String name = tuple.getFirst();
          String offheapResource = tuple.getSecond();

          new SovereignBuilder<>(Type.LONG, SystemTimeReference.class)
              .storage(storage)
              .alias(name)
              .offheapResourceName(offheapResource)
              .timeReferenceGenerator(new SystemTimeReference.Generator())
              .offheap()
              .build();
        });
  }

  private static class TestDatasetDiscoveryListener implements DatasetDiscoveryListener {
    private final List<String> aliases = new ArrayList<>();

    @Override
    public <K extends Comparable<K>> void foundExistingDataset(String diskResourceName, SovereignDataset<K> dataset) throws StoreException {
      String alias = dataset.getAlias();
      aliases.add(alias);
    }

    public List<String> getAliases() {
      return aliases;
    }
  }
}
