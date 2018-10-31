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

package com.terracottatech.store.client.stream;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import com.terracottatech.store.client.DatasetEntityClientService;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.server.ObservableDatasetEntityServerService;
import com.terracottatech.testing.AbstractEnterprisePassthroughTest;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.offheapresource.config.ResourceType;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.terracottatech.store.manager.DatasetManager.clustered;

@RunWith(Parameterized.class)
public abstract class AbstractBaseHAPassthroughTest extends AbstractEnterprisePassthroughTest {

  enum TestSetup {
      SINGLE_STRIPE
    , MULTI_STRIPE_SINGLE_ACTIVE_FAILOVER
    , MULTI_STRIPE_ALL_ACTIVE_FAILOVER
  }

  @Parameterized.Parameter
  public static TestSetup testSetup;

  @Parameterized.Parameters(name = "{0}")
  public static List<TestSetup> data() {
    return Arrays.asList(TestSetup.values());
  }

  public static List<String> stripeNames;

  @Override
  protected List<String> provideStripeNames() {
    switch (testSetup) {
      case SINGLE_STRIPE:
        stripeNames = Collections.singletonList("stripe");
        break;
      case MULTI_STRIPE_SINGLE_ACTIVE_FAILOVER:
        stripeNames = Arrays.asList("stripe1", "stripe2");
        break;
      case MULTI_STRIPE_ALL_ACTIVE_FAILOVER:
        stripeNames = Arrays.asList("stripe1", "stripe2");
        break;
      default:
        throw new UnsupportedOperationException();
    }

    return stripeNames;
  }

  @Override
  protected int provideStripeSize() {
    return 2;
  }

  private ObservableDatasetEntityServerService datasetEntityServerService;
  protected DatasetManager datasetManager;
  protected Dataset<Integer> dataset;
  protected DatasetConfiguration datasetConfiguration;
  protected DatasetWriterReader<Integer> employeeWriterReader;
  protected DatasetReader<Integer> employeeReader;
  protected static final String DATASET_NAME = "employee";

  @Before
  public void setup() throws Exception {
    datasetManager = clustered(buildClusterUri()).build();
    datasetConfiguration = datasetManager.datasetConfiguration().offheap("offheap").build();
    datasetManager.newDataset(DATASET_NAME, Type.INT, datasetConfiguration);
    dataset = datasetManager.getDataset(DATASET_NAME, Type.INT);
    employeeWriterReader = dataset.writerReader();
    employeeReader = dataset.reader();
  }

  @After
  public void tearDown() throws Exception {
    if (dataset != null) {
      dataset.close();
    }
    if (datasetManager != null) {
      datasetManager.close();
    }
  }

  @Override
  protected PassthroughTestHelpers.ServerInitializer provideExtraServerInitializer() {
    if (datasetEntityServerService == null) {
      datasetEntityServerService = new ObservableDatasetEntityServerService();
    }
    OffheapResourcesType offheapResourcesType = new OffheapResourcesType();
    addResourceType(offheapResourcesType, "offheap", 64, MemoryUnit.MB);
    return server -> {
      server.registerExtendedConfiguration(new OffHeapResourcesProvider(offheapResourcesType));
      server.registerClientEntityService(new DatasetEntityClientService());
      server.registerServerEntityService(datasetEntityServerService);
    };
  }

  private void addResourceType(OffheapResourcesType offheapResourcesType, String name, long value, MemoryUnit unit) {
    ResourceType resourceType = new ResourceType();
    resourceType.setName(name);
    resourceType.setValue(BigInteger.valueOf(value));
    resourceType.setUnit(unit);

    offheapResourcesType.getResource().add(resourceType);
  }

  void shutdownActiveAndWaitForPassiveToBecomeActive() {
    try {
      if (testSetup.equals(TestSetup.MULTI_STRIPE_ALL_ACTIVE_FAILOVER)) {
        for(PassthroughClusterControl cc : getListOfClusterControls()) {
          cc.terminateActive();
          cc.waitForActive();
        }
      } else {
        PassthroughClusterControl cc = getListOfClusterControls().get(0);
        cc.terminateActive();
        cc.waitForActive();
      }
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }

  void shutdownActive() {
    try {
      if (testSetup.equals(TestSetup.MULTI_STRIPE_ALL_ACTIVE_FAILOVER)) {
        for(PassthroughClusterControl cc : getListOfClusterControls()) {
          cc.terminateActive();
        }
      } else {
        PassthroughClusterControl cc = getListOfClusterControls().get(0);
        cc.terminateActive();
      }
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }
}
