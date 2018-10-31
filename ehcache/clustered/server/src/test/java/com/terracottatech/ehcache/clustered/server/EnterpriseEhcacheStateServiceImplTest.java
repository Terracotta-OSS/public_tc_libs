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

package com.terracottatech.ehcache.clustered.server;

import com.terracottatech.config.data_roots.DataDirectories;
import com.terracottatech.ehcache.clustered.server.services.frs.management.Management;
import com.terracottatech.ehcache.clustered.server.services.pool.ResourcePoolManagerConfiguration;
import com.terracottatech.ehcache.clustered.server.state.EnterpriseEhcacheStateServiceProvider;
import com.terracottatech.persistence.RestartablePlatformPersistence;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.DestroyInProgressException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.StateRepositoryMessageFactory;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.ServerSideServerStore;
import org.ehcache.clustered.server.state.InvalidationTracker;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;
import org.terracotta.offheapresource.OffHeapResources;

import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;
import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;
import com.terracottatech.ehcache.clustered.server.services.frs.EhcacheFRSService;
import com.terracottatech.ehcache.clustered.server.services.frs.EhcacheFRSServiceConfiguration;
import com.terracottatech.ehcache.clustered.server.services.frs.impl.EhcacheFRSServiceImpl;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.MapValue;
import static org.ehcache.clustered.server.offheap.OffHeapChainMap.chain;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsEmptyIterable.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author vmad
 */
public class EnterpriseEhcacheStateServiceImplTest {

  private static final String TEST_POOL_NAME1 = "pool1";
  private static final String TEST_POOL_NAME2 = "pool2";

  private static final String BASE_TEST_CACHE_NAME = "test-cache";
  private static final String TEST_RESOURCE_NAME = "test-resource";
  private static final int TEST_RESOURCE_SIZE = 1024 * 1024;

  private static final String TEST_ROOT_ID = "test-root";
  private static final String BASE_TIER_MANAGER_IDENTIFIER = "testCacheManager";
  private static final int MAX_TIERS = 4;
  private static final int MAX_TIER_MANAGERS = 3;


  private String testRootPath;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private ServiceRegistry serviceRegistryMock;
  private DataDirectories dataDirectoriesMock;

  @Before
  public void setUp() throws Exception {
    testRootPath = folder.newFolder().getAbsolutePath();
  }

  @Test
  public void testLoadExisting() throws Exception {
    testWrapper(false, 1, (ss, id) -> {
      try {
        ServerSideServerStore sss = ss.createStore(BASE_TEST_CACHE_NAME + 1, createServerStoreConfiguration(id % 2 == 0), true);
        sss.put(100L, chain(buffer(2)));
        assertThat(sss.get(100L), contains(element(2)));
      } catch (Exception e) {
        fail("Unexpected cluster exception :" + e.getMessage());
      }
    });

    testWrapper(true, 1, (ss, id) -> {
      //verify
      final Set<String> stores = ss.getStores();
      assertThat(stores.size(), is(1));
      assertThat(stores.contains(BASE_TEST_CACHE_NAME+1), is(true));
      ServerSideServerStore sss = ss.getStore(BASE_TEST_CACHE_NAME+1);
      try {
        assertThat(sss.get(100L), contains(element(2)));
      } catch (TimeoutException e) {
        fail("Data not recovered");
      }
    });
  }

  @Test
  public void testLoadStoreWithNonRestartableConfig() throws Exception {
    final String cache = BASE_TEST_CACHE_NAME + 1;
    testWrapper(false, 1, (ss, id) -> {
      try {
        ss.loadStore(cache, createNonRestartableServerStoreConfiguration(id % 2 == 0));
      } catch (Exception e) {
        fail("Unexpected cluster exception: " + e.getMessage());
      }
      assertThat(ss.getInvalidationTracker(cache), is(nullValue()));
    });

    testWrapper(true, 1, (ss, id) -> assertThat(ss.getInvalidationTracker(cache), is(nullValue())));
  }

  @Test
  public void testLoadStoreWithRestartableConfig() throws Exception {
    final String cache = BASE_TEST_CACHE_NAME + 1;
    testWrapper(false, 1, (ss, id) -> {
      try {
        ss.loadStore(cache, createServerStoreConfiguration(false));
      } catch (Exception e) {
        fail("Unexpected cluster exception: " + e.getMessage());
      }
      assertThat(ss.getInvalidationTracker(cache), is(not(nullValue())));
    });

    testWrapper(true, 1, (ss, id) -> assertThat(ss.getInvalidationTracker(cache), is(not(nullValue()))));
  }

  @Test
  public void testInvalidationTrackerRecovery() throws Exception {
    testWrapper(false, 1, (ss, id) -> {
      try {
        ss.createStore("foo", createServerStoreConfiguration(false), true);
      } catch (ConfigurationException e) {
        fail("Unexpected cluster exception :" + e.getMessage());
      }
      InvalidationTracker tracker = ss.getInvalidationTracker("foo");
      tracker.trackHashInvalidation(1L);
      tracker.trackHashInvalidation(2L);
      tracker.trackHashInvalidation(3L);
      tracker.setClearInProgress(true);
    });

    testWrapper(true, 1, (ss, id) -> {
      InvalidationTracker tracker = ss.getInvalidationTracker("foo");
      assertThat(tracker.getTrackedKeys(), containsInAnyOrder(1L, 2L, 3L, Long.MIN_VALUE));
      assertThat(tracker.isClearInProgress(), is(true));
    });
  }

  @Test
  public void testConcurrentMultiSegment() throws Exception {
    testWrapperConcurrentMultiSegment(false, 4, (ss, id) -> {
      try {
        assertThat(ss.getStore(BASE_TEST_CACHE_NAME + 1).getAndAppend(id, buffer(id)), emptyIterable());
        assertThat(ss.getStore(BASE_TEST_CACHE_NAME + 1).get(id), contains(element(id)));
      } catch (TimeoutException e) {
        fail("Unexpected cluster exception :" + e.getMessage());
      }
    });

    testWrapperConcurrentMultiSegment(true, 4, (ss, id) -> {
      try {
        assertThat(ss.getStore(BASE_TEST_CACHE_NAME + 1).get(id), contains(element(id)));
      } catch (TimeoutException e) {
        fail("Timeout Unexpected");
      }
    });
  }

  @Test
  public void testLoadExistingWithStateRepository() throws Exception {
    testWrapper(false, 1, (ss, id) -> {
      try {
        ss.createStore(BASE_TEST_CACHE_NAME+1, createServerStoreConfiguration(id % 2 == 0), true);
        StateRepositoryMessageFactory sr = new StateRepositoryMessageFactory(BASE_TEST_CACHE_NAME+1, "testSR");
        ss.getStateRepositoryManager().invoke(sr.putIfAbsentMessage("testSR", "VALUE_FOR_SR"));
      } catch (Exception e) {
        fail("Unexpected cluster exception :" + e.getMessage());
      }
    });

    testWrapper(true, 1, (ss, id) -> {
      //verify
      StateRepositoryMessageFactory sr1 = new StateRepositoryMessageFactory(BASE_TEST_CACHE_NAME+1, "testSR");
      EhcacheEntityResponse entityResponse = ss.getStateRepositoryManager().invoke(sr1.getMessage("testSR"));
      assertThat(entityResponse, instanceOf(MapValue.class));
      MapValue val = (MapValue)entityResponse;
      assertThat(val.getValue().toString(), containsString("VALUE_FOR_SR"));
      final Set<String> stores = ss.getStores();
      assertThat(stores.size(), is(1));
      assertThat(stores.contains(BASE_TEST_CACHE_NAME+1), is(true));
    });
  }

  @Test
  public void testLoadExistingMultiTierManagers() throws Exception {
    testWrapper(false, 2, (ss, id) -> {
      try {
        ss.createStore(BASE_TEST_CACHE_NAME+1, createServerStoreConfiguration(id % 2 == 0), true);
      } catch (Exception e) {
        fail("Unexpected cluster exception :" + e.getMessage());
      }
    });

    testWrapper(true, 2, (ss, id) -> {
      //verify
      final Set<String> stores = ss.getStores();
      assertThat(stores.size(), is(1));
      assertThat(stores.contains(BASE_TEST_CACHE_NAME+1), is(true));
    });
  }

  @Test
  public void testLoadExistingMultiTierManagersWithMultipleTiers() throws Exception {
    testWrapper(false, 3, (ss, id) -> {
      try {
        for (int i = 1; i <= MAX_TIERS; i++) {
          ss.createStore(BASE_TEST_CACHE_NAME+i, createServerStoreConfiguration(i % 2 == 0), true);
          StateRepositoryMessageFactory sr1 = new StateRepositoryMessageFactory(BASE_TEST_CACHE_NAME+i, "testSR1"+i);
          StateRepositoryMessageFactory sr2 = new StateRepositoryMessageFactory(BASE_TEST_CACHE_NAME+i, "testSR2"+i);
          ss.getStateRepositoryManager().invoke(sr1.putIfAbsentMessage("key", "FIRST_VALUE_FOR_KEY_IN_SR1" + i));
          ss.getStateRepositoryManager().invoke(sr2.putIfAbsentMessage("key1", "FIRST_VALUE_FOR_KEY1_IN_SR2" + i));
          ss.getStateRepositoryManager().invoke(sr1.putIfAbsentMessage("key", "MODIFIED_VALUE_FOR_KEY_IN_SR1" + i));
          ss.getStateRepositoryManager().invoke(sr2.putIfAbsentMessage("key2", "VALUE_IN_SR2" + i));
          ss.getStateRepositoryManager().invoke(sr2.putIfAbsentMessage("key3", "VALUE_IN_SR1" + i));
        }
      } catch (Exception e) {
        fail("Unexpected cluster exception :" + e.getMessage());
      }
    });

    testWrapper(true, 3, (ss, id) -> {
      //verify
      final Set<String> stores = ss.getStores();
      assertThat(stores.size(), is(MAX_TIERS));
      for (int i = 1; i <= MAX_TIERS; i++) {
        assertThat(stores.contains(BASE_TEST_CACHE_NAME + i), is(true));
      }

      for (int i = 1; i <= MAX_TIERS; i++) {
        final StateRepositoryMessageFactory sr1 = new StateRepositoryMessageFactory(BASE_TEST_CACHE_NAME + i, "testSR1" + i);
        final StateRepositoryMessageFactory sr2 = new StateRepositoryMessageFactory(BASE_TEST_CACHE_NAME + i, "testSR2" + i);
        EhcacheEntityResponse entityResponse1 = ss.getStateRepositoryManager().invoke(sr1.getMessage("key"));
        EhcacheEntityResponse entityResponse2 = ss.getStateRepositoryManager().invoke(sr2.getMessage("key1"));

        assertThat(entityResponse1, instanceOf(MapValue.class));
        final MapValue val1 = (MapValue)entityResponse1;
        assertThat(val1.getValue().toString(), containsString("FIRST_VALUE_FOR_KEY_IN_SR1" + i));

        assertThat(entityResponse2, instanceOf(MapValue.class));
        final MapValue val2 = (MapValue)entityResponse2;
        assertThat(val2.getValue().toString(), containsString("FIRST_VALUE_FOR_KEY1_IN_SR2" + i));
      }
    });
  }

  @Test
  public void testLoadExistingMultiTierManagerWithDifferentTierNames() throws Exception {
    testWrapper(false, 2, (ss, id) -> {
      try {
        ss.createStore(BASE_TEST_CACHE_NAME + 1, createServerStoreConfiguration(id % 2 == 0), true);
        // give a different cache name for the second one for different tier managers
        ss.createStore(BASE_TEST_CACHE_NAME + 2 + id, createServerStoreConfiguration(id % 2 == 0), true);
      } catch (Exception e) {
        fail("Unexpected cluster exception :" + e.getMessage());
      }
    });

    testWrapper(true, 2, (ss, id) -> {
      //verify
      final Set<String> stores = ss.getStores();
      assertThat(stores.size(), is(2));
      assertThat(stores.contains(BASE_TEST_CACHE_NAME + 1), is(true));
      assertThat(stores.contains(BASE_TEST_CACHE_NAME + 2 + id), is(true));
    });
  }

  @Test
  public void testPrepareForDestroyRecovery() throws Exception {
    testWrapper(false, 1, (ss, id) -> ss.prepareForDestroy());

    testWrapper(true, 1, (ss, id) -> {
      try {
        ss.validate(null);
        fail(DestroyInProgressException.class.getName() + " expected");
      } catch (DestroyInProgressException e) {
        //expected
      } catch (ClusterException e) {
        fail("Unexpected exception: " + e.getMessage());
      }
    });
  }

  @Test(expected = ConfigurationException.class)
  public void testConfigureRethorwsConfigurationException() throws Exception {
    createEhcacheFRSService();
    EnterpriseEhcacheStateServiceImpl service = new EnterpriseEhcacheStateServiceImpl(dataDirectoriesMock,
        mock(ClusterTierManagerConfiguration.class), serviceRegistryMock, new KeySegmentMapper(1), mock(EnterpriseEhcacheStateServiceProvider.DestroyCallback.class));

    when(serviceRegistryMock.getService(any(ResourcePoolManagerConfiguration.class))).thenThrow(new RuntimeException(new ConfigurationException("foo")));
    service.configure();
  }

  private ClusterTierManagerConfiguration createClusterTierConfiguration(int i) {
    return new ClusterTierManagerConfiguration(BASE_TIER_MANAGER_IDENTIFIER+i,
        createEnterpriseTierManager(i));
  }

  private ServerSideConfiguration createEnterpriseTierManager(int i) {
    RestartConfiguration restartConfiguration = new RestartConfiguration(TEST_ROOT_ID,
        RestartableOffHeapMode.FULL, BASE_TIER_MANAGER_IDENTIFIER + i);
    Map<String, ServerSideConfiguration.Pool> resourcePools = new HashMap<>();
    resourcePools.put(TEST_POOL_NAME1,
        new ServerSideConfiguration.Pool(TEST_RESOURCE_SIZE, TEST_RESOURCE_NAME));
    resourcePools.put(TEST_POOL_NAME2,
        new ServerSideConfiguration.Pool(TEST_RESOURCE_SIZE, TEST_RESOURCE_NAME));
    return new EnterpriseServerSideConfiguration(resourcePools, restartConfiguration);
  }

  private ServerStoreConfiguration createServerStoreConfiguration(boolean synchronousWrites) {
    PoolAllocation poolAllocation = new EnterprisePoolAllocation.DedicatedRestartable(TEST_RESOURCE_NAME,
        TEST_RESOURCE_SIZE, 0);

    return new ServerStoreConfiguration(poolAllocation,
        String.class.getName(),
        String.class.getName(),
        String.class.getName(),
        String.class.getName(), (synchronousWrites) ? Consistency.STRONG : Consistency.EVENTUAL);
  }

  private ServerStoreConfiguration createNonRestartableServerStoreConfiguration(boolean synchronousWrites) {
    PoolAllocation poolAllocation = new PoolAllocation.Dedicated(TEST_RESOURCE_NAME, TEST_RESOURCE_SIZE);

    return new ServerStoreConfiguration(poolAllocation,
        String.class.getName(),
        String.class.getName(),
        String.class.getName(),
        String.class.getName(), (synchronousWrites) ? Consistency.STRONG : Consistency.EVENTUAL);
  }

  private void testWrapper(boolean restart, int numMgrs,
                           BiConsumer<EnterpriseEhcacheStateServiceImpl, Integer> stateServiceConsumer)
      throws Exception {
    final EhcacheFRSService ehcacheFRSService = createEhcacheFRSService();

    List<EnterpriseEhcacheStateServiceImpl> stateServices = new ArrayList<>();
    for (int i = 1; i <= numMgrs; i++) {
      ClusterTierManagerConfiguration config = createClusterTierConfiguration(i);
      EnterpriseEhcacheStateServiceImpl enterpriseEhcacheStateService =
          new EnterpriseEhcacheStateServiceImpl(dataDirectoriesMock,
              config, serviceRegistryMock, new KeySegmentMapper(1), mock(EnterpriseEhcacheStateServiceProvider.DestroyCallback.class));
      stateServices.add(enterpriseEhcacheStateService);

      enterpriseEhcacheStateService.configure();
      if (restart) {
        enterpriseEhcacheStateService.loadExisting(createEnterpriseTierManager(i));
      }
    }

    // consume state services
    AtomicInteger idx = new AtomicInteger(1);
    stateServices.forEach((ss) -> stateServiceConsumer.accept(ss, idx.incrementAndGet()));

    // shutdown FRS service
    ehcacheFRSService.shutdown();
  }

  private void testWrapperConcurrentMultiSegment(boolean restart, int numTasks,
                           BiConsumer<EnterpriseEhcacheStateServiceImpl, Integer> stateServiceConsumer)
      throws Exception {
    final EhcacheFRSService ehcacheFRSService = createEhcacheFRSService();
    ClusterTierManagerConfiguration config = createClusterTierConfiguration(1);
    EnterpriseEhcacheStateServiceImpl enterpriseEhcacheStateService =
          new EnterpriseEhcacheStateServiceImpl(dataDirectoriesMock,
              config, serviceRegistryMock, new KeySegmentMapper(16), mock(EnterpriseEhcacheStateServiceProvider.DestroyCallback.class));

    enterpriseEhcacheStateService.configure();
    if (restart) {
      enterpriseEhcacheStateService.loadExisting(createEnterpriseTierManager(1));
    } else {
      enterpriseEhcacheStateService.createStore(BASE_TEST_CACHE_NAME + 1, createServerStoreConfiguration(true), true);
    }

    // consume state services
    ExecutorService executor = Executors.newFixedThreadPool(100);

    List<Future<?>> tasks = new ArrayList<>();
    for (int i = 0; i < numTasks; i++) {
      final int j = i;
      tasks.add(executor.submit(() -> stateServiceConsumer.accept(enterpriseEhcacheStateService, j)));
    }

    tasks.forEach((future) -> {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        fail("Unexpected Exception " + e.getMessage());
        e.printStackTrace();
      }
    });


    // shutdown FRS service
    ehcacheFRSService.shutdown();
  }

  private EhcacheFRSService createEhcacheFRSService() throws Exception {
    dataDirectoriesMock = mock(DataDirectories.class);
    when(dataDirectoriesMock.getDataDirectory(any())).thenReturn(Paths.get(this.testRootPath));
    when(dataDirectoriesMock.getDataDirectoryNames()).thenReturn(Collections.singleton(TEST_ROOT_ID));

    OffHeapResource offHeapResourceMock = mock(OffHeapResource.class);
    when(offHeapResourceMock.reserve(anyLong())).thenReturn(true);

    OffHeapResources offHeapResourcesMock = mock(OffHeapResources.class);
    when(offHeapResourcesMock.getAllIdentifiers()).thenReturn(Collections.singleton(OffHeapResourceIdentifier.identifier(TEST_RESOURCE_NAME)));
    when(offHeapResourcesMock.getOffHeapResource(any(OffHeapResourceIdentifier.class))).thenReturn(offHeapResourceMock);

    final ConcurrentMap<String, ResourcePoolManager> resourcePoolManagerMap = new ConcurrentHashMap<>();
    for(int i = 1; i <= MAX_TIER_MANAGERS; i++) {
      resourcePoolManagerMap.put(BASE_TIER_MANAGER_IDENTIFIER+i, new ResourcePoolManager(offHeapResourcesMock, () -> {}));
    }
    serviceRegistryMock = mock(ServiceRegistry.class);
    when(serviceRegistryMock.getService(argThat(ServiceConfigurationMatcherFactory.getMatcher(OffHeapResources.class)))).thenReturn(offHeapResourcesMock);
    EhcacheFRSService ehcacheFRSService = new EhcacheFRSServiceImpl(dataDirectoriesMock, serviceRegistryMock, false, new Management());
    when(serviceRegistryMock.getService(isA(EhcacheFRSServiceConfiguration.class))).thenReturn(ehcacheFRSService);
    when(serviceRegistryMock.getService(argThat(ServiceConfigurationMatcherFactory.getMatcher(RestartablePlatformPersistence.class)))).thenReturn(mock(
      RestartablePlatformPersistence.class));
    when(serviceRegistryMock.getService(isA(ResourcePoolManagerConfiguration.class))).thenAnswer((Answer<ResourcePoolManager>) invocationOnMock -> {
      final Object[] arguments = invocationOnMock.getArguments();
      @SuppressWarnings("unchecked")
      ResourcePoolManagerConfiguration resourcePoolManagerConfiguration = (ResourcePoolManagerConfiguration) arguments[0];
      return resourcePoolManagerMap.get(resourcePoolManagerConfiguration.getCacheManagerID());
    });

    return ehcacheFRSService;
  }

  @SuppressWarnings({ "cast", "RedundantCast" })
  private static ByteBuffer buffer(int i) {
    ByteBuffer buffer = ByteBuffer.allocate(i);
    while (buffer.hasRemaining()) {
      buffer.put((byte) i);
    }
    return (ByteBuffer) buffer.flip();        // made redundant in Java 9/10
  }

  private static Matcher<Element> element(final int i) {
    return new TypeSafeMatcher<Element>() {
      @Override
      protected boolean matchesSafely(Element item) {
        return item.getPayload().equals(buffer(i));
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("element containing buffer[" + i +"]");
      }
    };
  }

  private static class ServiceConfigurationMatcherFactory {
    static <T> ArgumentMatcher<ServiceConfiguration<T>> getMatcher(Class<T> clazz) {
      return o -> o != null && o.getServiceType().equals(clazz);
    }
  }
}
