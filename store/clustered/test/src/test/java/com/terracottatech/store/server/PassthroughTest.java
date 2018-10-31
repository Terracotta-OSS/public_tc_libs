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
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.client.ClusteredDataset;
import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.client.DatasetEntityClientService;
import com.terracottatech.store.client.InternalClusteredDatasetManager;
import com.terracottatech.store.internal.InternalDataset;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.testing.AbstractEnterprisePassthroughTest;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.terracotta.connection.Connection;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.offheapresource.config.ResourceType;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import static com.terracottatech.store.manager.DatasetManager.clustered;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

public class PassthroughTest extends AbstractEnterprisePassthroughTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ObservableDatasetEntityServerService datasetEntityServerService;
  protected URI clusterUri;
  protected DatasetManager datasetManager;
  protected Dataset<String> dataset;
  protected DatasetEntity<String> datasetEntity;

  @Before
  public void before() throws Exception {
    clusterUri = buildClusterUri();
    datasetManager = clustered(clusterUri).build();
    dataset = getTestDataset(datasetManager);
    replaceEntityBySpy();
  }

  @SuppressWarnings("unchecked")
  private void replaceEntityBySpy() throws Exception {
    if (dataset == null) {
      return;
    }
    ClusteredDataset<String> ds = (ClusteredDataset<String>) (dataset instanceof InternalDataset ?
        ((InternalDataset) dataset).getUnderlying() :
        dataset);
    Field field = ClusteredDataset.class.getDeclaredField("entity");
    field.setAccessible(true);
    datasetEntity = (DatasetEntity<String>) field.get(ds);
    datasetEntity = spy(datasetEntity);
    field.set(ds, datasetEntity);
  }

  @After
  public void after() {
    if (dataset != null) {
      dataset.close();
    }
    if (datasetManager != null) {
      datasetManager.close();
    }
  }

  protected Dataset<String> getTestDataset(DatasetManager datasetManager) throws StoreException {
    datasetManager.newDataset("address", Type.STRING, datasetManager.datasetConfiguration()
            .offheap("offheap")
            .build());
    return datasetManager.getDataset("address", Type.STRING);
  }

  @Override
  protected PassthroughTestHelpers.ServerInitializer provideExtraServerInitializer() {
    if (datasetEntityServerService == null) {
      datasetEntityServerService = new ObservableDatasetEntityServerService();
    }
    OffheapResourcesType offheapResourcesType = new OffheapResourcesType();
    addResourceType(offheapResourcesType, "offheap", 16, MemoryUnit.MB);
    return server -> {
      server.registerExtendedConfiguration(new OffHeapResourcesProvider(offheapResourcesType));
      server.registerClientEntityService(new DatasetEntityClientService());
      server.registerServerEntityService(datasetEntityServerService);
    };
  }

  @Override
  protected List<String> provideStripeNames() {
    return Collections.singletonList("stripe");
  }

  public ObservableDatasetEntityServerService getDatasetEntityServerService() {
    return datasetEntityServerService;
  }

  private void addResourceType(OffheapResourcesType offheapResourcesType, String name, long value, MemoryUnit unit) {
    ResourceType resourceType = new ResourceType();
    resourceType.setName(name);
    resourceType.setValue(BigInteger.valueOf(value));
    resourceType.setUnit(unit);

    offheapResourcesType.getResource().add(resourceType);
  }

  Connection getConnection() {
    return ((InternalClusteredDatasetManager) datasetManager).getConnection();
  }

  @SuppressWarnings({"varargs", "unchecked"})
  void resetDatasetEntity() {
    reset(datasetEntity);
  }

  @SuppressWarnings("varargs")
  @SafeVarargs
  static <T> Matcher<Iterable<? extends T>> containsOperations(Matcher<? super T>... matchers) {
    return Matchers.contains(matchers);
  }
}
