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

package com.terracottatech.sovereign.impl.dataset;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.test.data.Animals;
import com.terracottatech.sovereign.impl.AnimalsDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Record;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.BaseStream;

import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static com.terracottatech.sovereign.impl.SovereignDatasetStreamTestSupport.lockingConsumer;
import static java.util.Comparator.comparingLong;
import static java.util.stream.StreamSupport.*;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Performs tests on {@link RecordStream#spliterator()}.
 */
public class RecordStreamSpliteratorTest {

  private SovereignDataset<String> dataset;

  @Before
  public void setUp() throws Exception {
    /*
     * Create an Animal dataset with a short record lock timeout; this is required by
     * the locking tests
     */
    SovereignBuilder<String, SystemTimeReference> builder =
        AnimalsDataset.getBuilder(16 * 1024 * 1024, false)
            .recordLockTimeout(5L, TimeUnit.MILLISECONDS);
    this.dataset = AnimalsDataset.createDataset(builder);
    AnimalsDataset.addIndexes(this.dataset);
  }

  @After
  public void tearDown() throws Exception {
    dataset.getStorage().destroyDataSet(dataset.getUUID());
  }

  private RecordStream<String> getTestStream() {
    return this.dataset.records();
  }

  /**
   * Ensures a non-filtered, non-mutative stream does not lock.
   */
  @Test
  public void testPipelineLockingBasic() throws Exception {
    AtomicBoolean updated = new AtomicBoolean(false);
    AtomicBoolean timedOut = new AtomicBoolean(false);
    try (RecordStream<String> pipeline = this.getTestStream()) {
      assertThat(stream(pipeline
              .peek(lockingConsumer(this.dataset, "ocelot", updated, timedOut))
              .spliterator(), false)
              .count(),
          is((long)Animals.ANIMALS.size()));
    }

    assertTrue(updated.get());
    assertFalse(timedOut.get());
  }

  /**
   * Ensures a filtered, non-mutative stream does not lock.
   */
  @Test
  public void testPipelineLockingFilterNoMutation() throws Exception {
    AtomicBoolean updated = new AtomicBoolean(false);
    AtomicBoolean timedOut = new AtomicBoolean(false);
    try (RecordStream<String> pipeline = this.getTestStream()) {
      assertThat(stream(pipeline
              .filter(TAXONOMIC_CLASS.value().is("mammal"))
              .peek(lockingConsumer(this.dataset, "ocelot", updated, timedOut))
              .spliterator(), false)
              .count(),
          is(Animals.ANIMALS.stream().filter((a) -> a.getTaxonomicClass().equals("mammal")).count()));
    }

    assertTrue(updated.get());
    assertFalse(timedOut.get());
  }

  /**
   * Ensures a non-filtered, mutative stream locks.
   */
  @Ignore("Sovereign pipeline ending with spliterator cannot hold observed ManagedAction and become mutative")
  @Test
  public void testPipelineLockingNoFilterMutation() throws Exception {
  }

  /**
   * Ensures a filtered, mutative stream locks.
   */
  @Ignore("Sovereign pipeline ending with spliterator cannot hold observed ManagedAction and become mutative")
  @Test
  public void testPipelineLockingFilterMutation() throws Exception {
  }

  /**
   * This test ensures that the {@code RecordStream} structures underlying a simple spliterator
   * taken from the stream do not get reclaimed prematurely.
   * <p>
   * This test is designed to induce use of a {@link RecordSpliterator} instance.
   */
  @Test
  public void testPrematureReclamation() throws Exception {
    assertGcState(BaseStream::spliterator);
  }

  /**
   * This test ensures that the {@code RecordStream} structures underlying a spliterator
   * taken from the stream using a {@code filter} do not get reclaimed prematurely.
   * <p>
   * This test is designed to induce use of a {@link ManagedSpliterator} instance.
   */
  @Test
  public void testPrematureReclamationFilter() throws Exception {
    assertGcState((s) -> s.filter(TAXONOMIC_CLASS.value().is("mammal")).spliterator());
  }

  /**
   * This test ensures that the {@code RecordStream} structures underlying a spliterator
   * taken from the stream using a re-ordering operation do not get reclaimed prematurely.
   * <p>
   * This test is designed to induce use of a {@link PassThroughManagedSpliterator} instance.
   */
  @Test
  public void testPrematureReclamationWithSort() throws Exception {
    assertGcState((s) -> s.sorted(comparingLong(OBSERVATIONS.longValueOr(0L))).spliterator());
  }

  /**
   * This test ensures that the {@code RecordStream} structures underlying a spliterator
   * taken from a stream using both filtering and re-ordering operations to not get reclaimed prematurely.
   */
  @Test
  public void testPrematureReclamationWithFilteredSort() throws Exception {
    assertGcState((s) -> s.filter(TAXONOMIC_CLASS.value().is("mammal")).sorted(comparingLong(OBSERVATIONS.longValueOr(0L))).spliterator());
    assertGcState((s) -> s.sorted(comparingLong(OBSERVATIONS.longValueOr(0L))).filter(TAXONOMIC_CLASS.value().is("mammal")).spliterator());
  }

  /**
   * Test method used to check for premature garbage collection of the {@code RecordStream} underlying an
   * {@code Spliterator} derived from that stream.
   *
   * @param spliteratorSupplier the function producing the desired {@code Spliterator} from the test {@code RecordStream}
   */
  @SuppressWarnings("UnusedAssignment")
  private void assertGcState(Function<RecordStream<String>, Spliterator<Record<String>>> spliteratorSupplier)
      throws Exception {
    ReferenceQueue<RecordStream<?>> queue = new ReferenceQueue<>();
    final AtomicBoolean closed = new AtomicBoolean();

    RecordStream<String> stream = this.getTestStream();
    stream.selfClose(true).onClose(() -> closed.set(true));
    PhantomReference<RecordStream<?>> ref = new PhantomReference<>(stream, queue);

    Spliterator<Record<String>> spliterator = spliteratorSupplier.apply(stream);

    stream = null;        // unusedAssignment - explicit null of reference for garbage collection

    pollStreamGc(queue, closed);

    /*
     * Consumption of the Spliterator forces allocation of the RecordSpliterator used to
     * feed the Spliterator,  This causes the spliterator supplier fed to the Streams
     * framework to be de-referenced.
     */
    assertThat(spliterator.tryAdvance((r) -> assertThat(r, is(notNullValue()))), is(true));

    pollStreamGc(queue, closed);

    spliterator = null;      // unusedAssignment - explicit null of reference for garbage collection

    Reference<? extends RecordStream<?>> queuedRef;
    while ((queuedRef = queue.poll()) == null) {
      Thread.sleep(10L);
      System.gc();
      System.runFinalization();
    }
    assertThat(queuedRef, is(sameInstance(ref)));
    queuedRef.clear();

    assertThat(closed.get(), is(true));
  }

  private void pollStreamGc(ReferenceQueue<RecordStream<?>> queue, AtomicBoolean closed)
      throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      Thread.sleep(10L);
      System.gc();
      System.runFinalization();
      assertThat(queue.poll(), is(nullValue()));
      assertFalse(closed.get());
    }
  }
}
