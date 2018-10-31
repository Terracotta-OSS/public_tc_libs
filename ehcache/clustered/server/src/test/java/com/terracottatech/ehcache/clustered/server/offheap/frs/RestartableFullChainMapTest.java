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
package com.terracottatech.ehcache.clustered.server.offheap.frs;

import org.ehcache.clustered.server.offheap.ChainStorageEngine;
import org.junit.Test;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.Factory;

import com.terracottatech.ehcache.common.frs.ControlledTransactionRestartStore;
import com.terracottatech.frs.recovery.RecoveryException;

import java.nio.ByteBuffer;

import static org.ehcache.clustered.server.offheap.OffHeapChainMap.chain;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

@SuppressWarnings("unchecked")
public class RestartableFullChainMapTest extends BaseRestartableChainMapTest {
  @Test
  public void testPutFailure() throws Exception {
    loadFrsNewPageSource((m) -> {
      try {
        m.put("key", chain(buffer(10), buffer(10 * 1000), buffer(30 * 1024), buffer(10)));
        fail();
      } catch (OversizeMappingException e) {
        m.put("key", chain(buffer(10), buffer(11), buffer(10 * 1024), buffer(1024)));
      }
    }, 20, 10);

    loadFrsNewPageSource((m) -> assertThat(m.get("key"),
        contains(element(10), element(11), element(10 * 1024), element(1024))), 20, 10);
  }

  @Test
  public void testOversizeMappingExceptionThrowsException() throws Exception {
    loadFrsNewPageSource((m)-> {
      // minimum 50K usage
      for (int i = 0; i < 10; i++) {
        m.append(Integer.toString(i%50), buffer(50000+i%10));
      }
    }, 120, 120);

    try {
      // reduce the size and ensure that the value still exists
      loadFrsNewPageSource((m) -> {
        int evicted = 0;
        for (int i = 0; i < 10; i++) {
          if (m.get(Integer.toString(i)).isEmpty()) {
            evicted++;
          }
        }
        assertTrue(evicted > 0 && evicted < 50);
      }, 12, 12);
      fail("Must get an oversize mapping exception");
    } catch (RecoveryException e) {
      assertThat(e.getCause(), instanceOf(OversizeMappingException.class));
    }
  }

  @Test
  public void testPersistenceOnEviction() throws Exception {
    loadFrsNewPageSource((m)-> {
      // minimum 50K usage, eviction is inevitable
      for (int i = 0; i < 1000; i++) {
        m.append(Integer.toString(i%50), buffer(50+i%10));
      }
    }, 12, 12);

    loadFrsNewPageSource((m)-> {
      int evicted = 0;
      for (int i = 0; i < 50; i++) {
        if (m.get(Integer.toString(i)).isEmpty()) {
          evicted++;
        }
      }
      assertTrue(evicted > 0 && evicted < 50);
    }, 12, 12);
  }

  @Override
  protected OffHeapChainMapStripe<ByteBuffer, String>
  createStripe(ByteBuffer identifier, KeyToSegment<String> mapper, PageSource source,
               ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore) {
    Factory<? extends RestartableChainStorageEngine<ByteBuffer, String>> storageEngineFactory =
        RestartableOffHeapChainStorageEngine.createFactory(source,
            StringPortability.INSTANCE, KILOBYTES.toBytes(4), MEGABYTES.toBytes(8), false, false, identifier,
            restartStore, true);
    return new OffHeapChainMapStripe<>(identifier, mapper, source, StringPortability.INSTANCE,
        storageEngineFactory);
  }

  @Override
  protected boolean shouldTestCompaction() {
    return true;
  }

  @Override
  protected long getEngineFirstLsn(ChainStorageEngine<String> engine) {
    assertThat(engine, is(instanceOf(RestartableOffHeapChainStorageEngine.class)));
    final RestartableOffHeapChainStorageEngine<ByteBuffer, String> engine1 =
        (RestartableOffHeapChainStorageEngine<ByteBuffer, String>) engine;
    return engine1.getLowestLsn();
  }
}