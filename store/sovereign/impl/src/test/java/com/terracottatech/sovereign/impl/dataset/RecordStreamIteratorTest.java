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

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignDataset.Durability;
import com.terracottatech.test.data.Animals;
import com.terracottatech.test.data.Animals.Animal;
import com.terracottatech.sovereign.impl.AnimalsDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.BaseStream;

import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static com.terracottatech.sovereign.impl.SovereignDatasetStreamTestSupport.lockingConsumer;
import static com.terracottatech.sovereign.testsupport.RecordFunctions.alterRecord;
import static com.terracottatech.sovereign.testsupport.RecordFunctions.compute;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Performs tests on {@link RecordStream#iterator()}.
 */
public class RecordStreamIteratorTest {

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
      assertThat(collect(pipeline
              .peek(lockingConsumer(this.dataset, "ocelot", updated, timedOut))
              .iterator()),
          iterableWithSize(Animals.ANIMALS.size()));
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
      assertThat(collect(pipeline
              .filter(TAXONOMIC_CLASS.value().is("mammal"))
              .peek(lockingConsumer(this.dataset, "ocelot", updated, timedOut))
              .iterator()),
          iterableWithSize((int)Animals.ANIMALS.stream().filter((a) -> a.getTaxonomicClass().equals("mammal")).count()));
    }

    assertTrue(updated.get());
    assertFalse(timedOut.get());
  }

  /**
   * Ensures a non-filtered, mutative stream locks.
   */
  @Ignore("Sovereign pipeline ending with iterator cannot hold observed ManagedAction and become mutative")
  @Test
  public void testPipelineLockingNoFilterMutation() throws Exception {
  }

  /**
   * Ensures a filtered, mutative stream locks.
   */
  @Ignore("Sovereign pipeline ending with iterator cannot hold observed ManagedAction and become mutative")
  @Test
  public void testPipelineLockingFilterMutation() throws Exception {
  }

  /**
   * This test ensures that the {@code RecordStream} structures underlying a simple iterator
   * taken from the stream do not get reclaimed prematurely.
   * <p>
   * This test is designed to induce use of a {@link RecordSpliterator} instance.
   */
  @Test
  public void testPrematureReclamation() throws Exception {
    assertGcState(BaseStream::iterator);
  }

  /**
   * This test ensures that the {@code RecordStream} structures underlying an iterator
   * taken from the stream using a {@code filter} do not get reclaimed prematurely.
   * <p>
   * This test is designed to induce use of a {@link ManagedSpliterator} instance.
   */
  @Test
  public void testPrematureReclamationFilter() throws Exception {
    assertGcState((s) -> s.filter(TAXONOMIC_CLASS.value().is("mammal")).iterator());
  }

  /**
   * This test ensures that the {@code RecordStream} structures underlying an iterator
   * taken from the stream using a re-ordering operation do not get reclaimed prematurely.
   * <p>
   * This test is designed to induce use of a {@link PassThroughManagedSpliterator} instance.
   */
  @Test
  public void testPrematureReclamationWithSort() throws Exception {
    assertGcState((s) -> s.sorted(comparingLong(OBSERVATIONS.longValueOr(0L))).iterator());
  }

  /**
   * This test ensures that the {@code RecordStream} structures underlying an iterator
   * taken from a stream using both filtering and re-ordering operations to not get reclaimed prematurely.
   */
  @Test
  public void testtestPrematureReclamationWithFilteredSort() throws Exception {
    assertGcState((s) -> s.filter(TAXONOMIC_CLASS.value().is("mammal")).sorted(comparingLong(OBSERVATIONS.longValueOr(0L))).iterator());
    assertGcState((s) -> s.sorted(comparingLong(OBSERVATIONS.longValueOr(0L))).filter(TAXONOMIC_CLASS.value().is("mammal")).iterator());
  }

  /**
   * Test method used to check for premature garbage collection of the {@code RecordStream} underlying an
   * {@code Iterator} derived from that stream.
   *
   * @param iteratorSupplier the function producing the desired {@code Iterator} from the test {@code RecordStream}
   */
  @SuppressWarnings("UnusedAssignment")
  private void assertGcState(Function<RecordStream<String>, Iterator<Record<String>>> iteratorSupplier)
      throws InterruptedException {
    ReferenceQueue<RecordStream<?>> queue = new ReferenceQueue<>();
    final AtomicBoolean closed = new AtomicBoolean();

    RecordStream<String> stream = this.getTestStream();
    stream.selfClose(true).onClose(() -> closed.set(true));
    PhantomReference<RecordStream<?>> ref = new PhantomReference<>(stream, queue);

    Iterator<Record<String>> iterator = iteratorSupplier.apply(stream);

    stream = null;        // unusedAssignment - explicit null of reference for garbage collection

    pollStreamGc(queue, closed);

    /*
     * Consumption of the Iterator forces allocation of the RecordSpliterator used to
     * feed the Iterator,  This causes the spliterator supplier fed to the Streams
     * framework to be de-referenced.
     */
    assertThat(iterator.next(), is(notNullValue()));

    pollStreamGc(queue, closed);

    iterator = null;      // unusedAssignment - explicit null of reference for garbage collection

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

  @Test
  public void testBasicPrimaryIteration() throws Exception {
    final List<Record<String>> actualRecords;
    try (final RecordStream<String> stream = this.getTestStream()) {
      actualRecords = collect(stream.iterator());
    }
    final List<Animal> actualAnimals = actualRecords.stream().map(Animal::new).collect(toList());
    assertThat(actualAnimals, hasItems(Animals.ANIMALS.toArray(new Animal[Animals.ANIMALS.size()])));
  }

  @Test
  public void testPrimaryIterationWithInternalMutation() throws Exception {
    final List<Record<String>> expectedRecords;
    try (final RecordStream<String> stream = this.getTestStream()) {
      expectedRecords = collect(stream.iterator());
    }

    final List<Record<String>> actualRecords = new ArrayList<>(expectedRecords.size());
    try (final RecordStream<String> stream = this.getTestStream()) {
      final Iterator<Record<String>> recordIterator = stream.iterator();
      while (recordIterator.hasNext()) {
        final Record<String> record = recordIterator.next();
        actualRecords.add(record);
        if (record.get(TAXONOMIC_CLASS).orElse("").equals("bird")) {
          this.dataset.applyMutation(Durability.IMMEDIATE,
                                     record.getKey(),
                                     r -> true,
                                     alterRecord(compute(OBSERVATIONS, o -> OBSERVATIONS.newCell(o.value() + 1L))));
        }
      }
    }

    final List<String> actualRecordKeys = actualRecords.stream().map(Record::getKey).collect(toList());
    final List<String> expectedRecordKeys = expectedRecords.stream().map(Record::getKey).collect(toList());
    assertThat(actualRecordKeys, containsInAnyOrder(expectedRecordKeys.toArray(new String[expectedRecordKeys.size()])));
  }

  @Test
  public void testPrimaryIterationWithInterleavedAdditions() throws Exception {
    final List<Record<String>> expectedRecords;
    try (final RecordStream<String> stream = this.getTestStream()) {
      expectedRecords = collect(stream.iterator());
    }

    final String letters = "abcdefghijklmnopqrstuvwxyz";
    int additionCount = 0;
    final List<Record<String>> actualRecords = new ArrayList<>(expectedRecords.size());
    final List<Record<String>> addedRecords = new ArrayList<>();
    try (final RecordStream<String> stream = this.getTestStream()) {
      final Iterator<Record<String>> recordIterator = stream.iterator();
      while (recordIterator.hasNext()) {
        final Record<String> record = recordIterator.next();
        actualRecords.add(record);
        if (record.get(TAXONOMIC_CLASS).orElse("").equals("bird")) {
          final String newKey =
              String.format("%s%04d", letters.charAt(additionCount % letters.length()), ++additionCount);
          final Record<String> existingRecord =
              this.dataset.add(Durability.IMMEDIATE, newKey, TAXONOMIC_CLASS.newCell("fake"), OBSERVATIONS.newCell(0L));
          assertThat(existingRecord, is(nullValue()));
          final Record<String> newRecord = this.dataset.get(newKey);
          addedRecords.add(newRecord);
        }
      }
    }

    final List<String> actualRecordKeys = actualRecords.stream().map(Record::getKey).collect
      (toList());
    final List<String> expectedRecordKeys = expectedRecords.stream().map(Record::getKey).collect(toList());
    assertThat(actualRecordKeys, hasItems(expectedRecordKeys.toArray(new String[expectedRecordKeys
      .size()])));

    final Set<String> actualPostAddRecordKeys = this.dataset.records().map(Record::getKey).collect(toSet());
    final List<String> expectedPostAddRecordKeys = new ArrayList<>(expectedRecordKeys);
    expectedPostAddRecordKeys.addAll(addedRecords.stream().map(Record::getKey).collect(toList()));
    assertThat(actualPostAddRecordKeys,
        containsInAnyOrder(expectedPostAddRecordKeys.toArray(new String[expectedPostAddRecordKeys.size()])));
  }

  @Test
  public void testBasicSecondaryIteration() throws Exception {
    final List<Record<String>> actualRecords;
    try (final RecordStream<String> stream = this.getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      actualRecords = collect(stream.filter(TAXONOMIC_CLASS.value().is("mammal")).iterator());
      assertThat(assignedIndexCell.get(), is(equalTo(TAXONOMIC_CLASS)));
    }

    final List<Animal> actualAnimals = actualRecords.stream().map(Animal::new).collect(toList());
    assertThat(actualAnimals,
        hasItems(Animals.ANIMALS.stream().filter(a -> a.getTaxonomicClass().equals("mammal")).toArray(Animal[]::new)));
  }

  @Test
  public void testSecondaryIterationWithInterleavedAdditions() throws Exception {
    final List<Record<String>> expectedVisitedRecords;
    try (final RecordStream<String> stream = this.getTestStream()) {
      expectedVisitedRecords = collect(stream.filter(TAXONOMIC_CLASS.value().is("mammal")).iterator());
    }

    final String letters = "abcdefghijklmnopqrstuvwxyz";
    int additionCount = 0;
    final List<Record<String>> actualVisitedRecords = new ArrayList<>(expectedVisitedRecords.size());
    final List<Record<String>> addedRecords = new ArrayList<>();
    try (final RecordStream<String> stream = this.getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      final Iterator<Record<String>> recordIterator = stream.filter(TAXONOMIC_CLASS.value().is("mammal")).iterator();
      assertThat(assignedIndexCell.get(), is(equalTo(TAXONOMIC_CLASS)));

      while (recordIterator.hasNext()) {
        final Record<String> record = recordIterator.next();
        actualVisitedRecords.add(record);
        final String newKey =
            String.format("%s%04d", letters.charAt(additionCount % letters.length()), ++additionCount);
        final Record<String> existingRecord =
            this.dataset.add(Durability.IMMEDIATE, newKey, TAXONOMIC_CLASS.newCell("mammal"));
        assertThat(existingRecord, is(nullValue()));
        final Record<String> newRecord = this.dataset.get(newKey);
        addedRecords.add(newRecord);
      }
    }

    final List<String> actualRecordKeys = actualVisitedRecords.stream().map(Record::getKey).collect(toList());
    final List<String> expectedRecordKeys = expectedVisitedRecords.stream().map(Record::getKey).collect(toList());
    assertThat(actualRecordKeys, containsInAnyOrder(expectedRecordKeys.toArray(new String[expectedRecordKeys.size()])));

    final Set<String> actualPostAddRecordKeys =
        this.dataset.records().filter(TAXONOMIC_CLASS.value().is("mammal")).map(Record::getKey).collect(toSet());
    final List<String> expectedPostAddRecordKeys = new ArrayList<>(expectedRecordKeys);
    expectedPostAddRecordKeys.addAll(addedRecords.stream().map(Record::getKey).collect(toList()));
    assertThat(actualPostAddRecordKeys,
        containsInAnyOrder(expectedPostAddRecordKeys.toArray(new String[expectedPostAddRecordKeys.size()])));
  }

  /**
   * This test attempts to ensure that updates to a secondary index do not affect the records returned
   * by a task using that index.
   */
  @Test
  public void testSecondaryIterationWithIndexAffectingMutation() throws Exception {
    final List<Record<String>> expectedRecords;
    try (final RecordStream<String> stream = this.getTestStream()) {
      expectedRecords = collect(stream.filter(TAXONOMIC_CLASS.value().is("mammal")).iterator());
    }

    final List<Record<String>> actualRecords = new ArrayList<>(expectedRecords.size());
    final Map<String, String> taxonomicClassUpdates = new ConcurrentHashMap<>();
    try (final RecordStream<String> stream = this.getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      final Iterator<Record<String>> recordIterator = stream.filter(TAXONOMIC_CLASS.value().is("mammal")).iterator();
      assertThat(assignedIndexCell.get(), is(equalTo(TAXONOMIC_CLASS)));

      while (recordIterator.hasNext()) {
        final Record<String> record = recordIterator.next();
        actualRecords.add(record);
        /*
         * Force an update of the index being used for record retrieval ...
         */
        this.dataset.applyMutation(Durability.IMMEDIATE, record.getKey(), r -> true, alterRecord(compute(TAXONOMIC_CLASS,
                c -> {
                  final String newClass = new StringBuilder(c.value()).reverse().toString();
                  taxonomicClassUpdates.put(record.getKey(), newClass);
                  return TAXONOMIC_CLASS.newCell(newClass);
                })));
      }
    }

    final List<String> actualRecordKeys = actualRecords.stream().map(Record::getKey).collect(toList());
    final List<String> expectedRecordKeys = expectedRecords.stream().map(Record::getKey).collect(toList());
    assertThat(actualRecordKeys, containsInAnyOrder(expectedRecordKeys.toArray(new String[expectedRecordKeys.size()])));

    this.dataset.records().forEach(r -> {
      final String updatedTaxonomicClass = taxonomicClassUpdates.get(r.getKey());
      if (updatedTaxonomicClass != null) {
        assertThat(r.get(TAXONOMIC_CLASS).orElse(""), is(equalTo(updatedTaxonomicClass)));
      }
    });
  }

  private static <E> List<E> collect(final Iterator<E> iterator) {
    assert iterator != null;
    final List<E> elements = new ArrayList<>(128);
    iterator.forEachRemaining(elements::add);
    return elements;
  }
}
