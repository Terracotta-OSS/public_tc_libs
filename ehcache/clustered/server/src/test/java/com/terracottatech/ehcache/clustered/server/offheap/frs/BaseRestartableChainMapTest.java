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

import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.server.offheap.ChainStorageEngine;
import org.ehcache.clustered.server.offheap.OffHeapChainMap;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import com.terracottatech.ehcache.common.frs.ControlledTransactionRestartStore;
import com.terracottatech.ehcache.common.frs.FrsCodecFactory;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.RegisterableObjectManager;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_LSNGAP_MAX_LOAD;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_LSNGAP_MIN_LOAD;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_RUN_INTERVAL;
import static org.ehcache.clustered.server.offheap.OffHeapChainMap.chain;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.collection.IsEmptyIterable.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;

/**
 * Tests {@link RestartableChainStorageEngine}.
 */
@SuppressWarnings("unchecked")
public abstract class BaseRestartableChainMapTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File folder;
  private Properties properties;

  @Before
  public void setUp() throws Exception {
    folder = temporaryFolder.newFolder();
    properties = new Properties();
  }

  @Test
  public void testSingleBufferPut() throws Exception {
    loadFrs((m) -> {
      m.append("key", buffer(1));
      m.append("key1", buffer(2));
      assertThat(m.get("key"), contains(element(1)));
      assertThat(m.get("key1"), contains(element(2)));
      assertThat(m.get("foo"), emptyIterable());
      assertThat(m.get("key"), contains(element(1)));
    });
  }

  @Test
  public void testSingleBufferPersistentPut() throws Exception {
    loadFrs((m) -> {
      m.append("key11", buffer(1));
      m.append("key12", buffer(2));
      assertThat(m.get("key11"), contains(element(1)));
      assertThat(m.get("key12"), contains(element(2)));
      assertThat(m.get("foo"), emptyIterable());
    });
    loadFrs((m) -> {
      assertThat(m.get("key11"), contains(element(1)));
      assertThat(m.get("key12"), contains(element(2)));
      assertThat(m.get("foo"), emptyIterable());
      m.append("foo", buffer(4));
      assertThat(m.get("foo"), contains(element(4)));
    });
  }

  @Test
  public void testSimpleMultiKeyReplace() throws Exception {
    loadFrs((m) -> {
      m.getAndAppend("k1", buffer(1));
      m.getAndAppend("k2", buffer(2));
      m.replaceAtHead("k1", chain(buffer(1)), chain());
      m.replaceAtHead("k2", chain(buffer(2)), chain());
      assertThat(m.get("k1"), is(emptyIterable()));
      assertThat(m.get("k2"), is(emptyIterable()));

      m.getAndAppend("k1", buffer(301));
      m.replaceAtHead("k1", chain(buffer(301)), chain(buffer(9)));
      assertThat(m.get("k1"), contains(element(9)));
    });

    loadFrs((m) -> {
      assertThat(m.get("k1"), contains(element(9)));
      assertThat(m.get("k2"), is(emptyIterable()));
    });
  }

  @Test
  public void testSingleBufferPersistentPutMultiSegment() throws Exception {
    final String[] keys = {"key11", "jkk", "gfh", "pfq", "ss", "ppp", "qqq", "ccc"} ;
    loadFrsMultiSegment((s, mapper) -> {
      List<OffHeapChainMap<String>> maps = new ArrayList<>(s.segments());

      int i = 1;
      for (String key : keys) {
        OffHeapChainMap<String> m = maps.get(mapper.getSegmentForKey(key));
        m.append(key, buffer(i));
        assertThat(m.get(key), contains(element(i)));
        assertThat(m.get("junk"), emptyIterable());
        i++;
      }
    });

    loadFrsMultiSegment((s, mapper) -> {
      List<OffHeapChainMap<String>> maps = new ArrayList<>(s.segments());
      int i = 1;
      for (String key : keys) {
        OffHeapChainMap<String> m = maps.get(mapper.getSegmentForKey(key));
        assertThat(m.get(key), contains(element(i)));
        assertThat(m.get("junk"), emptyIterable());
        i++;
      }
    });
  }

  @Test
  public void testPersistentGetAndAppendToEmptyChain() throws Exception {
    loadFrs((m) -> {
      assertThat(m.getAndAppend("foo", buffer(3)), emptyIterable());
      assertThat(m.get("foo"), contains(element(3)));
    });
    loadFrs((m) -> assertThat(m.get("foo"), contains(element(3))));
  }

  @Test
  public void testPersistentMultiAppendToChain() throws Exception {
    loadFrs((m) -> {
      m.append("foo3", buffer(3));
      m.append("foo3", buffer(2));
      m.append("foo3", buffer(1));
      m.append("foo2", buffer(4));
      m.append("foo2", buffer(1));
      assertThat(m.get("foo3"), contains(element(3), element(2), element(1)));
      assertThat(m.get("foo"), emptyIterable());
      assertThat(m.get("foo2"), contains(element(4), element(1)));
    });

    loadFrs((m) -> {
      assertThat(m.get("foo3"), contains(element(3), element(2), element(1)));
      assertThat(m.get("foo"), emptyIterable());
      assertThat(m.get("foo2"), contains(element(4), element(1)));
    });
  }

  @Test
  public void testPersistentReplace() throws Exception {
    loadFrs((m) -> {
      m.append("foo3", buffer(1));
      m.append("foo3", buffer(2));
      m.append("foo3", buffer(3));

      m.replaceAtHead("foo3", chain(buffer(1)), chain(buffer(42)));
      assertThat(m.get("foo3"), contains(element(42), element(2), element(3)));

      m.append("foo2", buffer(1));
      m.append("foo2", buffer(2));

      m.replaceAtHead("foo2", chain(buffer(1), buffer(3)), chain(buffer(42)));
      assertThat(m.get("foo2"), contains(element(1), element(2)));
    });

    loadFrs((m) -> {
      m.append("foo3", buffer(11));
      assertThat(m.get("foo3"), contains(element(42), element(2), element(3), element(11)));
      assertThat(m.get("foo2"), contains(element(1), element(2)));
    });
  }

  @Test
  public void testPersistentRemove() throws Exception {
    loadFrs((m) -> {
      m.append("foo3", buffer(1));
      m.append("foo3", buffer(2));

      m.replaceAtHead("foo3", chain(buffer(1)), chain(buffer(42)));
      assertThat(m.get("foo3"), contains(element(42), element(2)));

      m.append("foo2", buffer(1));
      m.append("foo2", buffer(2));

      m.replaceAtHead("foo2", chain(buffer(1), buffer(3)), chain(buffer(42)));
      assertThat(m.get("foo2"), contains(element(1), element(2)));
      m.append("foo2", buffer(3));
      m.append("foo2", buffer(4));
      assertThat(m.get("foo2"), contains(element(1), element(2), element(3), element(4)));

      m.replaceAtHead("foo3", chain(buffer(42), buffer(2)), chain());
      assertThat(m.get("foo3"), emptyIterable());
    });

    loadFrs((m) -> {
      assertThat(m.get("foo3"), emptyIterable());
      assertThat(m.get("foo2"), contains(element(1), element(2), element(3), element(4)));

      m.append("foo3", buffer(11));
      m.append("foo3", buffer(21));

      m.replaceAtHead("foo3", chain(buffer(11)), chain(buffer(22)));
      assertThat(m.get("foo3"), contains(element(22), element(21)));

      // break the chain now as this will test the breaking of contiguous chain post recovery
      m.replaceAtHead("foo2", chain(buffer(1), buffer(2)), chain(buffer(10240)));

      // try to create other elements to ensure no corruption
      m.append("foo2", buffer(101));
      assertThat(m.getAndAppend("foo2", buffer(102)), contains(element(10240), element(3), element(4), element(101)));
      assertThat(m.get("foo2"), contains(element(10240), element(3), element(4), element(101), element(102)));

      assertThat(m.getAndAppend("foo3", buffer(1)), contains(element(22), element(21)));
      assertThat(m.get("foo3"), contains(element(22), element(21), element(1)));
    });
  }

  @Test
  public void testPersistentRemoveMultiLevel() throws Exception {
    loadFrs((m) -> {
      m.append("foo3", buffer(1));
      m.append("foo3", buffer(2));

      m.replaceAtHead("foo3", chain(buffer(1)), chain(buffer(42)));
      assertThat(m.get("foo3"), contains(element(42), element(2)));

      m.append("foo2", buffer(1));
      m.append("foo2", buffer(2));
      assertThat(m.get("foo2"), contains(element(1), element(2)));
    });

    loadFrs((m) -> {
      assertThat(m.get("foo3"), contains(element(42), element(2)));
      m.replaceAtHead("foo3", chain(buffer(42), buffer(2)), chain());
      assertThat(m.get("foo3"), emptyIterable());

      m.append("foo2", buffer(3));
      m.append("foo2", buffer(4));

      assertThat(m.get("foo2"), contains(element(1), element(2), element(3), element(4)));
      m.replaceAtHead("foo2", chain(buffer(1), buffer(2)), chain(buffer(11), buffer(22), buffer(31)));
      assertThat(m.get("foo2"), contains(element(11), element(22), element(31), element(3), element(4)));
    });

    loadFrs((m) -> {
      assertThat(m.get("foo2"), contains(element(11), element(22), element(31), element(3), element(4)));
      assertThat(m.get("foo3"), emptyIterable());
      assertThat(m.get("foo1"), emptyIterable());

      m.append("foo1", buffer(1));
      assertThat(m.getAndAppend("foo1", buffer(2)), contains(element(1)));

      m.append("foo3", buffer(2));
      m.append("foo3", buffer(3));

      m.replaceAtHead("foo2", chain(buffer(11), buffer(22), buffer(31), buffer(3)), chain());

      assertThat(m.get("foo1"), contains(element(1), element(2)));
      assertThat(m.get("foo2"), contains(element(4)));
      assertThat(m.get("foo3"), contains(element(2), element(3)));
    });
  }

  @Test
  public void testMismatchingReplace() throws Exception {
    loadFrs((m) -> {
      m.append("foo1", buffer(1));
      m.append("foo1", buffer(2));
      m.replaceAtHead("foo1", chain(buffer(1), buffer(3)), chain(buffer(11)));
      assertThat(m.get("foo1"), contains(element(1), element(2)));
      m.append("foo2", buffer(3));
      assertThat(m.get("foo2"), contains(element(3)));
    });

    loadFrs((m) -> {
      assertThat(m.get("foo1"), contains(element(1), element(2)));
      m.append("foo1", buffer(3));
      assertThat(m.get("foo1"), contains(element(1), element(2), element(3)));
      m.append("foo2", buffer(3));
      assertThat(m.get("foo2"), contains(element(3), element(3)));
    });
  }

  @Test
  public void testPersistentMultiReplace() throws Exception {
    loadFrs((m) -> {
      m.append("foo2", buffer(10));
      m.replaceAtHead("foo2", chain(buffer(10)), chain(buffer(11), buffer(1)));
      m.replaceAtHead("foo2", chain(buffer(11), buffer(1)), chain());
      assertThat(m.get("foo2"), emptyIterable());
    });

    loadFrs((m) -> {
      assertThat(m.get("foo2"), emptyIterable());
      m.append("foo2", buffer(1));
      m.getAndAppend("foo2", buffer(2));
      m.replaceAtHead("foo2", chain(buffer(1), buffer(2)), chain(buffer(13), buffer(1)));
      m.replaceAtHead("foo2", chain(buffer(13)), chain());
      m.replaceAtHead("foo2", chain(buffer(1)), chain());
      assertThat(m.get("foo2"), emptyIterable());
    });

    loadFrs((m) -> {
      assertThat(m.get("foo2"), emptyIterable());
      m.append("foo2", buffer(1));
      assertThat(m.get("foo2"), contains(element(1)));
    });
  }

  @Test
  public void testPersistentPut() throws Exception {
    loadFrs((m) -> {
      m.append("foo1", buffer(1));
      m.put("foo1", chain(buffer(3), buffer(2)));
      m.replaceAtHead("foo1", chain(buffer(3), buffer(2)), chain(buffer(11)));
      assertThat(m.get("foo1"), contains(element(11)));

      m.put("foo2", chain(buffer(10), buffer(12)));
      m.append("foo2", buffer(3));
      m.replaceAtHead("foo2", chain(buffer(10)), chain(buffer(11), buffer(1)));
      assertThat(m.get("foo2"), contains(element(11), element(1), element(12), element(3)));
    });

    loadFrs((m) -> {
      assertThat(m.getAndAppend("foo1", buffer(1)), contains(element(11)));
      assertThat(m.get("foo1"), contains(element(11), element(1)));

      assertThat(m.getAndAppend("foo2", buffer(13)), contains(element(11), element(1), element(12), element(3)));
      assertThat(m.get("foo2"), contains(element(11), element(1), element(12), element(3), element(13)));
      m.replaceAtHead("foo2", chain(buffer(11), buffer(1), buffer(12), buffer(3)), chain());
      assertThat(m.get("foo2"), contains(element(13)));
      m.replaceAtHead("foo2", chain(buffer(13)), chain());
      assertThat(m.get("foo2"), emptyIterable());
    });

    loadFrs((m) -> {
      assertThat(m.get("foo1"), contains(element(11), element(1)));
      m.put("foo1", chain(buffer(2)));
      assertThat(m.get("foo1"), contains(element(2)));

      m.append("foo2", buffer(1));
      m.put("foo2", chain(buffer(13)));
      m.replaceAtHead("foo2", chain(buffer(13)), chain());
      assertThat(m.get("foo2"), emptyIterable());
    });
  }

  @Test
  public void testAppendsInALoops() throws Exception {
    loadFrsNewPageSource((m)-> {
      for (int i = 1; i < 1000; i++) {
        m.append(Integer.toString(i), buffer(1));
      }
    }, 1024, 1024);

    loadFrsNewPageSource(((m) -> assertThat(m.get(Integer.toString(999)), contains(element(1)))), 1024, 1024);
  }

  @Test
  public void testCompactionMultiSegment() throws Exception {
    if (!shouldTestCompaction()) {
      return;
    }
    setFastCompactionProperties();
    loadFrsMultiSegment((s, mapper)-> {
      final List<OffHeapChainMap<String>> maps = new ArrayList<>(s.segments());
      final OffHeapChainMap<String> m = maps.get(mapper.getSegmentForKey(Integer.toString(0)));

      // keep the first one valid continuously to verify that compaction is happening.
      m.append(Integer.toString(0), buffer(1));
      m.replaceAtHead(Integer.toString(0), chain(buffer(1)), chain(buffer(2), buffer(3)));
      assertThat(m.get(Integer.toString(0)), contains(element(2), element(3)));
      final ChainStorageEngine<String> engine = m.getStorageEngine();
      final long firstLsn = getEngineFirstLsn(engine);

      for (int i = 1; i < 1000; i++) {
        final OffHeapChainMap<String> m1 = maps.get(mapper.getSegmentForKey(Integer.toString(i)));
        m1.append(Integer.toString(i), buffer(10));
        if (i % 2 == 0) {
          m1.replaceAtHead(Integer.toString(i), chain(buffer(10)), chain());
        } else {
          for (int k = 1; k <= 10; k++) {
            m1.append(Integer.toString(i), buffer(k));
          }
          m1.replaceAtHead(Integer.toString(i), chain(buffer(10), buffer(1), buffer(2), buffer(3), buffer(4), buffer(5)),
              chain());
        }
      }
      // give a bit more pause
      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException ignored) {
      }
      // assert that compaction has happened
      assertThat(getEngineFirstLsn(engine), is(greaterThan(firstLsn)));
    });

    loadFrsMultiSegment((s, mapper) -> {
      final List<OffHeapChainMap<String>> maps = new ArrayList<>(s.segments());
      final OffHeapChainMap<String> m0 = maps.get(mapper.getSegmentForKey(Integer.toString(0)));
      final OffHeapChainMap<String> m2 = maps.get(mapper.getSegmentForKey(Integer.toString(2)));
      final OffHeapChainMap<String> m999 = maps.get(mapper.getSegmentForKey(Integer.toString(999)));

      assertThat(m0.get(Integer.toString(0)), contains(element(2), element(3)));
      assertThat(m2.get(Integer.toString(2)), emptyIterable());
      assertThat(m999.get(Integer.toString(999)), contains(element(6), element(7), element(8), element(9), element(10)));
    });
  }

  protected long getEngineFirstLsn(ChainStorageEngine<String> engine) {
    return 0;
  }

  protected boolean shouldTestCompaction() {
    return false;
  }

  protected void setFastCompactionProperties() {
    properties.setProperty(COMPACTOR_RUN_INTERVAL.shortName(), "1");
    properties.setProperty(COMPACTOR_LSNGAP_MIN_LOAD.shortName(), "0.95");
    properties.setProperty(COMPACTOR_LSNGAP_MAX_LOAD.shortName(), "0.98");
  }

  protected void loadFrs(Consumer<OffHeapChainMap<String>> chainMapConsumer) throws Exception {
    loadFrsWithPageSource(chainMapConsumer, new UnlimitedPageSource(new OffHeapBufferSource()));
  }

  protected void loadFrsNewPageSource(Consumer<OffHeapChainMap<String>> chainMapConsumer, int sizeInKB, int chunkSizeInKB)
      throws Exception {
    loadFrsWithPageSource(chainMapConsumer, new UpfrontAllocatingPageSource(new OffHeapBufferSource(),
        KILOBYTES.toBytes(sizeInKB), KILOBYTES.toBytes(chunkSizeInKB)));
  }

  private void loadFrsWithPageSource(Consumer<OffHeapChainMap<String>> chainMapConsumer, PageSource source)
      throws Exception {
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
    ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
        createTransactionalRestartStore(objectManager);
    final ByteBuffer identifier = FrsCodecFactory.toByteBuffer("test1");

    final OffHeapChainMapStripe<ByteBuffer, String> stripe = createStripe(identifier, new StringMapper(),
        source, restartStore);
    objectManager.registerStripe(identifier, stripe);
    restartStore.startup().get();

    chainMapConsumer.accept(stripe.segments().get(0));

    restartStore.shutdown();
  }

  protected void loadFrsMultiSegment(BiConsumer<OffHeapChainMapStripe<ByteBuffer, String>, StringMapper> chainMapConsumer)
      throws Exception {
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
    ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
        createTransactionalRestartStore(objectManager);
    final ByteBuffer identifier = FrsCodecFactory.toByteBuffer("test1");

    final StringMapper mapper = new StringMapper(4);
    final OffHeapChainMapStripe<ByteBuffer, String> stripe = createStripe(identifier, mapper,
        new UnlimitedPageSource(new OffHeapBufferSource()), restartStore);

    objectManager.registerStripe(identifier, stripe);
    restartStore.startup().get();

    chainMapConsumer.accept(stripe, mapper);

    restartStore.shutdown();
  }

  protected abstract OffHeapChainMapStripe<ByteBuffer,String>
  createStripe(ByteBuffer identifier, KeyToSegment<String> mapper, PageSource source,
               ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore);

  private ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createTransactionalRestartStore(ObjectManager<ByteBuffer,
      ByteBuffer, ByteBuffer> objectManager) throws
      RestartStoreException, IOException {
    return new ControlledTransactionRestartStore<>(RestartStoreFactory.createStore(objectManager, folder, properties));
  }

  private RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> createObjectManager() {
    return new RegisterableObjectManager<>();
  }

  @SuppressWarnings({ "cast", "RedundantCast" })
  static ByteBuffer buffer(int i) {
    ByteBuffer buffer = ByteBuffer.allocate(i);
    while (buffer.hasRemaining()) {
      buffer.put((byte) i);
    }
    return (ByteBuffer) buffer.flip();        // made redundant in Java 9/10
  }

  static Matcher<Element> element(final int i) {
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

  static final class StringMapper implements KeyToSegment<String> {
    final int segments;

    public StringMapper() {
      this.segments = 1;
    }

    public StringMapper(int segments) {
      this.segments = segments;
    }

    @Override
    public int getSegmentForKey(String key) {
      return key.hashCode() % segments;
    }

    @Override
    public int getSegments() {
      return segments;
    }
  }
}
