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

import org.junit.Test;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.Factory;

import com.terracottatech.ehcache.common.frs.ControlledTransactionRestartStore;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

public class RestartablePartialChainMapTest extends BaseRestartableChainMapTest {
  boolean lowCache = false;
  @Test
  public void testHybridCacheEviction() throws Exception {
    lowCache = true;
    loadFrsNewPageSource((m)-> {
      for (int i = 1; i <= 256; i++) {
        m.append(Integer.toString(i), buffer(i));
      }
      for (int i = 1; i <= 256; i++) {
        assertThat(m.get(Integer.toString(i)), contains(element(i)));
      }
      for (int i = 256; i >= 1; i--) {
        assertThat(m.get(Integer.toString(i)), contains(element(i)));
      }
    }, 120, 120);

    loadFrsNewPageSource((m)-> {
      int evicted = 0;
      for (int i = 1; i <= 256; i++) {
        if (m.get(Integer.toString(i)).isEmpty()) {
          evicted++;
        } else {
          assertThat(m.get(Integer.toString(i)), contains(element(i)));
        }
      }
      assertThat(evicted, is(0));
    }, 120, 120);
  }

  @Override
  protected OffHeapChainMapStripe<ByteBuffer, String>
  createStripe(ByteBuffer identifier, KeyToSegment<String> mapper, PageSource source,
               ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore) {
    Factory<? extends RestartableChainStorageEngine<ByteBuffer, String>> storageEngineFactory =
        RestartableHybridChainStorageEngine.createFactory(source,
            StringPortability.INSTANCE, KILOBYTES.toBytes(4), MEGABYTES.toBytes(8), false, false, identifier,
            restartStore, true, new UpfrontAllocatingPageSource(new OffHeapBufferSource(),
                KILOBYTES.toBytes(lowCache ? 8 : 1024), KILOBYTES.toBytes(lowCache ? 8 : 1024)));
    return new OffHeapChainMapStripe<>(identifier, mapper, source, StringPortability.INSTANCE,
        storageEngineFactory);
  }
}
