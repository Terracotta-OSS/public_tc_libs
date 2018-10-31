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
package com.terracottatech.ehcache.internal.config.xml;

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.SharedClusteredResourcePoolImpl;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.hamcrest.Matchers;
import org.junit.Test;

import com.terracottatech.ehcache.clustered.client.internal.config.EnterpriseClusteredDedicatedResourcePoolImpl;
import com.terracottatech.ehcache.clustered.client.internal.config.EnterpriseClusteredSharedResourcePoolImpl;
import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;
import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.disk.config.FastRestartStoreResourcePool;

import java.net.URL;
import java.util.Collection;
import java.util.Map;

import static com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode.FULL;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static com.terracottatech.ehcache.internal.config.xml.EnterpriseResourceConfigurationParserTestHelper.assertElementXml;

public class EnterpriseRestartabilityXmlTest {

  private static final URL uri = EnterpriseRestartabilityXmlTest.class.getResource("/ehcache-restartable.xml");
  private static final URL uri_2 = EnterpriseRestartabilityXmlTest.class.getResource("/ehcache-without-optional-restartable-attributes.xml");
  private static final URL uri_3 = EnterpriseRestartabilityXmlTest.class.getResource("/ehcache-ee-complete.xml");

  @Test
  public void testClientSideRestartabilityConfiguration() throws Exception {
    final Configuration configuration = new XmlConfiguration(uri);

    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations =
        configuration.getServiceCreationConfigurations();
    assertThat(serviceCreationConfigurations, is(not(Matchers.empty())));

    CacheManagerPersistenceConfiguration clusteringServiceConfiguration =
        ServiceUtils.findSingletonAmongst(CacheManagerPersistenceConfiguration.class, serviceCreationConfigurations);
    assertThat(clusteringServiceConfiguration, is(notNullValue()));

    Map<String, CacheConfiguration<?, ?>> cacheConfigurations = configuration.getCacheConfigurations();
    CacheConfiguration<?, ?> cacheConfiguration = cacheConfigurations.get("frs-cache");
    ResourcePools resourcePools = cacheConfiguration.getResourcePools();
    ResourceType<?> next = resourcePools.getResourceTypeSet().iterator().next();
    ResourcePool resourcePool = resourcePools.getPoolForResource(next);

    assertThat(resourcePool, instanceOf(FastRestartStoreResourcePool.class));
    FastRestartStoreResourcePool frsPool = (FastRestartStoreResourcePool) resourcePool;
    assertThat(frsPool.getUnit(), is(MemoryUnit.MB));
    assertThat(frsPool.getSize(), is(2L));
  }

  @Test
  public void testServerSideRestartabilityConfiguration() throws Exception {
    final Configuration configuration = new XmlConfiguration(uri);

    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations =
        configuration.getServiceCreationConfigurations();
    assertThat(serviceCreationConfigurations, is(not(Matchers.empty())));

    ClusteringServiceConfiguration clusteringServiceConfiguration =
        ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
    assertThat(clusteringServiceConfiguration, is(notNullValue()));

    EnterpriseServerSideConfiguration serverConfiguration =
        (EnterpriseServerSideConfiguration) clusteringServiceConfiguration.getServerConfiguration();
    RestartConfiguration restartConfiguration = serverConfiguration.getRestartConfiguration();
    assertThat(restartConfiguration.getFrsIdentifier(), is("frs1"));
    assertThat(restartConfiguration.getRestartableLogRoot(), is("root1"));
    assertThat(restartConfiguration.getOffHeapMode(), is(FULL));
  }

  @Test
  public void testOptionalAttributesInSchema() throws Exception {
    final Configuration configuration = new XmlConfiguration(uri_2);

    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations =
        configuration.getServiceCreationConfigurations();
    assertThat(serviceCreationConfigurations, is(not(Matchers.empty())));

    ClusteringServiceConfiguration clusteringServiceConfiguration =
        ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
    assertThat(clusteringServiceConfiguration, is(notNullValue()));

    EnterpriseServerSideConfiguration serverConfiguration =
        (EnterpriseServerSideConfiguration) clusteringServiceConfiguration.getServerConfiguration();
    RestartConfiguration restartConfiguration = serverConfiguration.getRestartConfiguration();
    assertThat(restartConfiguration.getFrsIdentifier(), is(nullValue()));
    assertThat(restartConfiguration.getRestartableLogRoot(), is("root1"));
    assertThat(restartConfiguration.getOffHeapMode(), is(FULL));
  }

  @Test
  public void testDedicatedNonRestartableCacheConfiguration() throws Exception {
    final Configuration configuration = new XmlConfiguration(uri);

    Map<String, CacheConfiguration<?, ?>> cacheConfigurations = configuration.getCacheConfigurations();
    CacheConfiguration<?, ?> cacheConfiguration = cacheConfigurations.get("dedicated-cache");
    ResourcePools resourcePools = cacheConfiguration.getResourcePools();
    ResourceType<?> next = resourcePools.getResourceTypeSet().iterator().next();
    ResourcePool resourcePool = resourcePools.getPoolForResource(next);

    assertThat(resourcePool, instanceOf(DedicatedClusteredResourcePoolImpl.class));
    DedicatedClusteredResourcePoolImpl enterprisePool = (DedicatedClusteredResourcePoolImpl) resourcePool;
    PoolAllocation poolAllocation = enterprisePool.getPoolAllocation();
    assertThat(poolAllocation, instanceOf(PoolAllocation.Dedicated.class));
    PoolAllocation.Dedicated dedicatedPoolAllocation = (PoolAllocation.Dedicated) poolAllocation;
    assertThat(dedicatedPoolAllocation.getResourceName(), is("primary-server-resource"));
    assertThat(dedicatedPoolAllocation.getSize(), is(MemoryUnit.valueOf("MB").toBytes(4)));
  }

  @Test
  public void testDedicatedRestartableCacheConfiguration() throws Exception {
    final Configuration configuration = new XmlConfiguration(uri);

    Map<String, CacheConfiguration<?, ?>> cacheConfigurations = configuration.getCacheConfigurations();
    CacheConfiguration<?, ?> cacheConfiguration = cacheConfigurations.get("dedicated-restartable-cache");
    ResourcePools resourcePools = cacheConfiguration.getResourcePools();
    ResourceType<?> next = resourcePools.getResourceTypeSet().iterator().next();
    ResourcePool resourcePool = resourcePools.getPoolForResource(next);

    assertThat(resourcePool, instanceOf(EnterpriseClusteredDedicatedResourcePoolImpl.class));
    EnterpriseClusteredDedicatedResourcePoolImpl enterprisePool = (EnterpriseClusteredDedicatedResourcePoolImpl) resourcePool;
    PoolAllocation poolAllocation = enterprisePool.getPoolAllocation();
    assertThat(poolAllocation, instanceOf(EnterprisePoolAllocation.DedicatedRestartable.class));
    EnterprisePoolAllocation.DedicatedRestartable dedicatedRestartablePoolAllocation =
        (EnterprisePoolAllocation.DedicatedRestartable) poolAllocation;
    assertThat(dedicatedRestartablePoolAllocation.getResourceName(), is("secondary-server-resource"));
    assertThat(dedicatedRestartablePoolAllocation.getDataPercent(), is(88));
    assertThat(dedicatedRestartablePoolAllocation.getSize(), is(MemoryUnit.valueOf("MB").toBytes(4)));
  }

  @Test
  public void testAnotherDedicatedRestartableCacheConfiguration() throws Exception {
    final Configuration configuration = new XmlConfiguration(uri);

    Map<String, CacheConfiguration<?, ?>> cacheConfigurations = configuration.getCacheConfigurations();
    CacheConfiguration<?, ?> cacheConfiguration = cacheConfigurations.get("another-dedicated-restartable-cache");
    ResourcePools resourcePools = cacheConfiguration.getResourcePools();
    ResourceType<?> next = resourcePools.getResourceTypeSet().iterator().next();
    ResourcePool resourcePool = resourcePools.getPoolForResource(next);

    assertThat(resourcePool, instanceOf(EnterpriseClusteredDedicatedResourcePoolImpl.class));
    EnterpriseClusteredDedicatedResourcePoolImpl enterprisePool = (EnterpriseClusteredDedicatedResourcePoolImpl) resourcePool;
    PoolAllocation poolAllocation = enterprisePool.getPoolAllocation();
    assertThat(poolAllocation, instanceOf(EnterprisePoolAllocation.DedicatedRestartable.class));
    EnterprisePoolAllocation.DedicatedRestartable dedicatedRestartablePoolAllocation =
        (EnterprisePoolAllocation.DedicatedRestartable) poolAllocation;
    assertThat(dedicatedRestartablePoolAllocation.getResourceName(), nullValue());
    assertThat(dedicatedRestartablePoolAllocation.getSize(), is(MemoryUnit.valueOf("MB").toBytes(8)));
    assertThat(dedicatedRestartablePoolAllocation.getDataPercent(), is(EnterprisePoolAllocation.UNINITIALIZED_DATA_PERCENT));
  }

  @Test
  public void testSharedRestartableCacheConfiguration() throws Exception {
    final Configuration configuration = new XmlConfiguration(uri);

    Map<String, CacheConfiguration<?, ?>> cacheConfigurations = configuration.getCacheConfigurations();
    CacheConfiguration<?, ?> cacheConfiguration = cacheConfigurations.get("shared-restartable-cache");
    ResourcePools resourcePools = cacheConfiguration.getResourcePools();
    ResourceType<?> next = resourcePools.getResourceTypeSet().iterator().next();
    ResourcePool resourcePool = resourcePools.getPoolForResource(next);

    assertThat(resourcePool, instanceOf(EnterpriseClusteredSharedResourcePoolImpl.class));
    EnterpriseClusteredSharedResourcePoolImpl enterprisePool = (EnterpriseClusteredSharedResourcePoolImpl) resourcePool;
    PoolAllocation poolAllocation = enterprisePool.getPoolAllocation();
    assertThat(poolAllocation, instanceOf(EnterprisePoolAllocation.SharedRestartable.class));
    EnterprisePoolAllocation.SharedRestartable sharedRestartablePoolAllocation =
        (EnterprisePoolAllocation.SharedRestartable) poolAllocation;
    assertThat(sharedRestartablePoolAllocation.getResourcePoolName(), is("shared-pool1"));
  }

  @Test
  public void testAnotherSharedRestartableCacheConfiguration() throws Exception {
    final Configuration configuration = new XmlConfiguration(uri);

    Map<String, CacheConfiguration<?, ?>> cacheConfigurations = configuration.getCacheConfigurations();
    CacheConfiguration<?, ?> cacheConfiguration = cacheConfigurations.get("another-shared-restartable-cache");
    ResourcePools resourcePools = cacheConfiguration.getResourcePools();
    ResourceType<?> next = resourcePools.getResourceTypeSet().iterator().next();
    ResourcePool resourcePool = resourcePools.getPoolForResource(next);

    assertThat(resourcePool, instanceOf(EnterpriseClusteredSharedResourcePoolImpl.class));
    EnterpriseClusteredSharedResourcePoolImpl enterprisePool = (EnterpriseClusteredSharedResourcePoolImpl) resourcePool;
    PoolAllocation poolAllocation = enterprisePool.getPoolAllocation();
    assertThat(poolAllocation, instanceOf(EnterprisePoolAllocation.SharedRestartable.class));
    EnterprisePoolAllocation.SharedRestartable sharedRestartablePoolAllocation =
        (EnterprisePoolAllocation.SharedRestartable) poolAllocation;
    assertThat(sharedRestartablePoolAllocation.getResourcePoolName(), is("shared-pool2"));
  }

  @Test
  public void testSharedNonRestartableCacheConfiguration() throws Exception {
    final Configuration configuration = new XmlConfiguration(uri);

    Map<String, CacheConfiguration<?, ?>> cacheConfigurations = configuration.getCacheConfigurations();
    CacheConfiguration<?, ?> cacheConfiguration = cacheConfigurations.get("shared-cache");
    ResourcePools resourcePools = cacheConfiguration.getResourcePools();
    ResourceType<?> next = resourcePools.getResourceTypeSet().iterator().next();
    ResourcePool resourcePool = resourcePools.getPoolForResource(next);

    assertThat(resourcePool, instanceOf(SharedClusteredResourcePoolImpl.class));
    SharedClusteredResourcePoolImpl enterprisePool = (SharedClusteredResourcePoolImpl) resourcePool;
    PoolAllocation poolAllocation = enterprisePool.getPoolAllocation();
    assertThat(poolAllocation, instanceOf(PoolAllocation.Shared.class));
    PoolAllocation.Shared sharedPoolAllocation = (PoolAllocation.Shared) poolAllocation;
    assertThat(sharedPoolAllocation.getResourcePoolName(), is("shared-pool"));
  }

  @Test
  public void testTranslate() {
    Configuration config = new XmlConfiguration(uri_3);
    XmlConfiguration xmlConfig = new XmlConfiguration(config);
    assertElementXml(uri_3, xmlConfig.toString());
  }
}
