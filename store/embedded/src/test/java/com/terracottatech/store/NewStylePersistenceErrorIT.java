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

import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.manager.DatasetManager;

import java.util.HashMap;
import java.util.Map;

import static com.terracottatech.store.configuration.PersistentStorageEngine.*;
import static com.terracottatech.store.definition.CellDefinition.define;
import static com.terracottatech.store.manager.DatasetManager.embedded;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@SuppressWarnings("try")
public class NewStylePersistenceErrorIT {
  private static final String OFFHEAP1 = "offheap1";
  private static final String OFFHEAP2 = "offheap2";
  private static final String DISK1 = "disk1";
  private static final String DISK2 = "disk2";

  @Rule
  public TemporaryFolder dataDir = new TemporaryFolder();

  private DatasetManager createDatasetManager() throws Exception {
    return embedded()
        .disk(DISK1, dataDir.newFolder().toPath(), REOPEN_OR_NEW)
        .disk(DISK2, dataDir.newFolder().toPath(), REOPEN_OR_NEW)
        .offheap(OFFHEAP1, 4, MemoryUnit.MB)
        .offheap(OFFHEAP2, 4, MemoryUnit.MB)
        .build();
  }

  @Test
  public void testDatasetStorageDefaulting() throws Exception {
    DatasetManager mgr = createDatasetManager();
    try (TestDatasets datasets = new TestDatasets(mgr, null, "dataset1")) {
      DatasetWriterReader<Long> access = datasets.getDataset("dataset1").writerReader();

      CellDefinition<String> foo = define("foo", Type.STRING);
      access.add(1L, foo.newCell("fooVal"));
      assertThat(access.get(1L).isPresent(), is(true));
    } finally {
      mgr.destroyDataset("dataset1");
      mgr.close();
    }
  }

  @Test
  public void testMultipleDatasetsWithSameStorageType() throws Exception {
    DatasetManager mgr = createDatasetManager();
    try (TestDatasets datasets = new TestDatasets(mgr, FRS, "dataset1", "dataset2")) {
      DatasetWriterReader<Long> access1 = datasets.getDataset("dataset1").writerReader();
      DatasetWriterReader<Long> access2 = datasets.getDataset("dataset2").writerReader();

      CellDefinition<String> foo = define("foo", Type.STRING);
      access1.add(1L, foo.newCell("fooVal"));
      assertThat(access1.get(1L).isPresent(), is(true));
      access2.add(10L, foo.newCell("fooVal1"));
      assertThat(access2.get(10L).isPresent(), is(true));
      assertThat(access2.get(1L).isPresent(), is(false));
    } finally {
      mgr.destroyDataset("dataset1");
      mgr.destroyDataset("dataset2");
      mgr.close();
    }
  }

  @Test
  public void testMultipleDataTypesWithDifferentStorageTypesBeforeDestroy() throws Exception {
    DatasetManager mgr = createDatasetManager();
    try (TestDatasets datasets = new TestDatasets(mgr, HYBRID, "dataset1")) {
      try (TestDatasets datasets2 = new TestDatasets(mgr, FRS, "dataset2")) {
        fail("Datasets with incompatible storage type must not be created " + datasets2.datasets.get("dataset2"));
      } catch (IllegalArgumentException e) {
        assertThat(e.getMessage(), containsString("Incompatible Storage Types"));
      }
      DatasetWriterReader<Long> access = datasets.getDataset("dataset1").writerReader();

      CellDefinition<String> foo = define("foo", Type.STRING);
      access.add(1L, foo.newCell("fooVal"));
      assertThat(access.get(1L).isPresent(), is(true));
    } finally {
      mgr.destroyDataset("dataset1");
      mgr.close();
    }
  }

  private static class TestDatasets implements AutoCloseable {
    private final DatasetManager datasetManager;
    private final Map<String, Dataset<Long>> datasets;

    public TestDatasets(DatasetManager mgr, PersistentStorageType storageType, String...names) throws StoreException {
      assert(names.length > 0 && names.length <= 2);
      this.datasetManager = mgr;
      this.datasets = new HashMap<>();
      int i = 1;
      for (String name : names) {
        String offheap = (i == 1) ? OFFHEAP1 : OFFHEAP2;
        String disk = (i == 1) ? DISK1 : DISK2;
        i++;

        if (storageType != null) {
          datasetManager.newDataset(name, Type.LONG, datasetManager.datasetConfiguration()
              .disk(disk, storageType).offheap(offheap).build());
        } else {
          datasetManager.newDataset(name, Type.LONG, datasetManager.datasetConfiguration()
              .disk(disk).offheap(offheap).build());
        }
        datasets.put(name, datasetManager.getDataset(name, Type.LONG));
      }
    }

    public Dataset<Long> getDataset(String name) throws StoreException {
      return datasets.get(name);
    }

    @Override
    public void close() throws Exception {
      datasets.forEach((x, y) -> y.close());
      datasets.clear();
    }
  }
}
