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
package com.terracottatech.store.systemtest;

import com.terracottatech.testing.rules.EnterpriseExternalCluster;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.Properties;
import java.util.function.Supplier;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;

public class BaseSystemTest {

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  protected static final String CLUSTER_OFFHEAP_RESOURCE = "primary-server-resource";
  protected static final String SECONDARY_OFFHEAP_RESOURCE = "secondary-server-resource";
  protected static final String CLUSTER_TINY_OFFHEAP_RESOURCE = "tiny-server-resource";

  protected static final String CLUSTER_DISK_RESOURCE = "cluster-disk-resource";
  protected static final String SECONDARY_DISK_RESOURCE = "secondary-disk-resource";

  protected static EnterpriseExternalCluster initCluster(String configFileName) {
    String SERVICE_CONFIG = ServiceConfigHelper.getServiceConfig(BaseSystemTest.class, configFileName);
    return newCluster(1).withPlugins(SERVICE_CONFIG).build();
  }

  protected static final EnterpriseExternalCluster initClusterWithPassive(String configFileName, int numOfservers) {
    String SERVICE_CONFIG = ServiceConfigHelper.getServiceConfig(BaseSystemTest.class, configFileName);
    return newCluster(numOfservers).withPlugins(SERVICE_CONFIG).build();
  }

  protected static EnterpriseExternalCluster initClusterWithPassive(String configFileName, int numOfServers,
                                                                    Supplier<Properties> serverPropertiesSupplier) {
    String SERVICE_CONFIG = ServiceConfigHelper.getServiceConfig(BaseSystemTest.class, configFileName);
    return newCluster(numOfServers).withPlugins(SERVICE_CONFIG).withServerProperties(serverPropertiesSupplier).build();
  }

  protected static final EnterpriseExternalCluster initClusterWithPassives(String configFileName, int passives) {
    String SERVICE_CONFIG = ServiceConfigHelper.getServiceConfig(BaseSystemTest.class, configFileName);
    return newCluster(passives + 1).withPlugins(SERVICE_CONFIG).build();
  }

  protected static final EnterpriseExternalCluster initMultiStripeCluster(String configFileName) {
    String SERVICE_CONFIG = ServiceConfigHelper.getServiceConfig(BaseSystemTest.class, configFileName);
    return newCluster(1, 1).withPlugins(SERVICE_CONFIG).build();
  }

  protected static final EnterpriseExternalCluster initConsistentClusterWithPassive(String configFileName, int voterCount, int numOfservers) {
    String SERVICE_CONFIG = ServiceConfigHelper.getServiceConfig(BaseSystemTest.class, configFileName);
    return newCluster(numOfservers).withPlugins(SERVICE_CONFIG).withFailoverPriorityVoterCount(voterCount).build();
  }

}
