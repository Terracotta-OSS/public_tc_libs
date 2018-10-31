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
package com.terracottatech.store.server;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetKeyTypeMismatchException;
import com.terracottatech.store.DatasetMissingException;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.DatasetManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("try")
@RunWith(Parameterized.class)
public class DatasetEntityLifecycleTest extends PassthroughTest {

  private final List<String> stripeNames;

  public DatasetEntityLifecycleTest(List<String> stripeNames) {
    this.stripeNames = stripeNames;
  }

  @Override
  protected List<String> provideStripeNames() {
    return stripeNames;
  }

  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] {Collections.singletonList("stripe"), Arrays.asList("stripe1", "stripe2") };
  }

  @Test
  public void testListDatasets() throws Exception {
    assertThat(datasetManager.listDatasets().size(), is(0));

    DatasetConfiguration configuration = datasetManager.datasetConfiguration().offheap("offheap").build();
    datasetManager.newDataset("datasetLong", Type.LONG, configuration);
    datasetManager.newDataset("datasetString", Type.STRING, configuration);

    Map<String, Type<?>> datasets = datasetManager.listDatasets();
    assertThat(datasets.size(), is(2));
    assertThat(datasets.get("datasetLong"), is(Type.LONG));
    assertThat(datasets.get("datasetString"), is(Type.STRING));

    datasetManager.destroyDataset("datasetString");
    datasets = datasetManager.listDatasets();
    assertThat(datasets.size(), is(1));
    assertThat(datasets.get("datasetLong"), is(Type.LONG));

    datasetManager.destroyDataset("datasetLong");
    datasets = datasetManager.listDatasets();
    assertThat(datasets.size(), is(0));
  }

  @Test
  public void createDataset() throws Exception {
    assertThat(datasetManager.newDataset("address", Type.STRING, datasetManager.datasetConfiguration().offheap("offheap").build()), is(true));

    try (Dataset<String> dataset = datasetManager.getDataset("address", Type.STRING)) {
      assertNotNull(dataset);
    }
    try (Dataset<String> dataset = datasetManager.getDataset("nonexistent", Type.STRING)) {
      fail("Expected an exception");
    } catch (DatasetMissingException e) {
      // Test success
    }
  }

  @Test
  public void getDatasetWithWrongTypeFails() throws Exception {
    assertThat(datasetManager.newDataset("address", Type.STRING, datasetManager.datasetConfiguration().offheap("offheap").build()), is(true));

    try (Dataset<Long> dataset = datasetManager.getDataset("address", Type.LONG)) {
      assertNotNull(dataset);
      fail("Expected an exception");
    } catch (DatasetKeyTypeMismatchException e) {
      // Test success
    }
  }

  @Test
  public void createExistingDatasetFails() throws Exception {
    assertThat(datasetManager.newDataset("address", Type.STRING, datasetManager.datasetConfiguration().offheap("offheap").build()), is(true));

    assertThat(datasetManager.newDataset("address", Type.STRING, datasetManager.datasetConfiguration().offheap("offheap").build()), is(false));
  }

  @Test
  public void destroyExistingDataset() throws Exception {
    assertThat(datasetManager.newDataset("address", Type.STRING, datasetManager.datasetConfiguration().offheap("offheap").build()), is(true));

    try (Dataset<String> dataset = datasetManager.getDataset("address", Type.STRING)) {
      assertNotNull(dataset);
    }

    assertTrue(datasetManager.destroyDataset("address"));

    try (Dataset<String> dataset = datasetManager.getDataset("address", Type.STRING)) {
      fail("Expected an exception");
    } catch (DatasetMissingException e) {
      // Test success
    }
  }

  @Test
  public void destroyUnknownDataset() throws Exception {
    try (Dataset<String> dataset = datasetManager.getDataset("address", Type.STRING)) {
      fail("Expected an exception");
    } catch (DatasetMissingException e) {
      // Test success
    }

    assertFalse(datasetManager.destroyDataset("address"));

    try (Dataset<String> dataset = datasetManager.getDataset("address", Type.STRING)) {
      fail("Expected an exception");
    } catch (DatasetMissingException e) {
      // Test success
    }
  }

  @Test
  public void destroyDatasetInUseFails() throws Exception {
    assertThat(datasetManager.newDataset("address", Type.STRING, datasetManager.datasetConfiguration().offheap("offheap").build()), is(true));

    try (Dataset<String> dataset = datasetManager.getDataset("address", Type.STRING)) {
      assertNotNull(dataset);

      try {
        datasetManager.destroyDataset("address");
        fail("Expected an exception");
      } catch (StoreException e) {
        // Test success
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void sneakyBytesKeyFails() throws Exception {
    Type bytesType = Type.BYTES;
    try {
      datasetManager.newDataset("address", bytesType, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build());
      fail("Expected an exception");
    } catch (StoreRuntimeException e) {
      // Test success
    }
  }

  @Override
  protected Dataset<String> getTestDataset(DatasetManager datasetManager) throws StoreException {
    return null;
  }
}
