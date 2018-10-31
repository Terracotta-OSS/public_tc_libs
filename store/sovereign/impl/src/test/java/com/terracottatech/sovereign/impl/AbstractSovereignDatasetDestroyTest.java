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

package com.terracottatech.sovereign.impl;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.description.SovereignDatasetDescription;
import com.terracottatech.store.internal.InternalRecord;
import com.terracottatech.test.data.Animals.Animal;
import com.terracottatech.sovereign.impl.dataset.Catalog;
import com.terracottatech.sovereign.impl.indexing.SimpleIndex;
import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.MemorySpace;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.impl.model.SovereignIndexMap;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.sovereign.spi.Space;
import com.terracottatech.sovereign.spi.store.Context;
import com.terracottatech.sovereign.spi.store.DataContainer;
import com.terracottatech.sovereign.spi.store.Locator;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Answers;
import org.terracotta.offheapstore.util.PhysicalMemory;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.STATUS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static com.terracottatech.sovereign.testsupport.RecordFunctions.alterRecord;
import static com.terracottatech.sovereign.testsupport.RecordFunctions.assign;
import static com.terracottatech.tool.WaitForAssert.assertThatEventually;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Provides tests over {@link com.terracottatech.sovereign.impl.SovereignDatasetImpl#dispose()}.
 */
public abstract class AbstractSovereignDatasetDestroyTest {

  protected final int maximumPageSize = 1024 * 1024;
  protected final int maxSpace = maximumPageSize * 16;

  private SovereignDataset<String> dataset;

  @Before
  public void setUp() throws Exception {
    this.dataset = this.newDataset();
  }

  @After
  public void tearDown() throws Exception {
    if (this.dataset != null && !this.dataset.isDisposed()) {
      this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
    this.dataset = null;

    Runtime runtime = Runtime.getRuntime();
    for (int i = 0; i < 10; i++) {
      runtime.gc();
      runtime.runFinalization();
      Thread.yield();
    }
  }

  abstract SovereignDataset<String> newDataset();

  /**
   * Ensures {@link com.terracottatech.sovereign.SovereignDataset#isDisposed() isDisposed}
   */
  @Test
  public void testBasicDispose() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    assertTrue(dataset.isDisposed());
  }

  @Test
  public void testDisposedDispose() throws Exception {
    SovereignStorage<?, ?> storage = this.dataset.getStorage();
    storage.destroyDataSet(dataset.getUUID());
    storage.destroyDataSet(dataset.getUUID());
  }

  @Test
  public void testFullDispose() throws Exception {
    AnimalsDataset.addIndexes(this.dataset);

    /*
     * Add a non-sorted index over an unused serial number cell just to create an index
     * that isn't based on an BtreeIndexMap.
     */
    final CellDefinition<Long> serialNumber = CellDefinition.define("serialNumber", Type.LONG);
    this.dataset.getIndexing().createIndex(serialNumber, SovereignIndexSettings.btree()).call();

    final Space<?, ?> space = ((SovereignDatasetImpl<String>) this.dataset).getUsage();
    assumeThat(space, is(instanceOf(MemorySpace.class)));
    final MemorySpace memorySpace = (MemorySpace) space;
    assertFalse(memorySpace.isDropped());

    /*
     * Peer into the MemorySpace instance to get the list of indexes about which it knows.
     */
    final Field memorySpaceMapsField = MemorySpace.class.getDeclaredField("maps");
    memorySpaceMapsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Set<SovereignIndexMap<?, ?>> memorySpaceMaps = (Set<SovereignIndexMap<?, ?>>) memorySpaceMapsField.get(memorySpace);

    // unchecked
    assumeThat(memorySpaceMaps, isA(Set.class));

    /*
     * Make sure all of the expected indexes are known and not disposed
     */
    final List<SovereignIndexMap<?, ?>> underlyingIndexes = new ArrayList<>();
    final List<SovereignIndex<?>> indexes = this.dataset.getIndexing().getIndexes();
    for (final SovereignIndex<?> index : indexes) {
      assertTrue(index.isLive());
      assumeThat(index, is(instanceOf(SimpleIndex.class)));
      final SovereignIndexMap<?, ?> underlyingIndex = ((SimpleIndex)index).getUnderlyingIndex();
      underlyingIndex.estimateSize();   // Will fail if dropped
      underlyingIndexes.add(underlyingIndex);
      assertThat(memorySpaceMaps, hasItem(underlyingIndex));
    }

    /*
     * Drop the SovereignDataset ...
     */
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    /*
     * Check for disposal of referenced objects.
     */
    assertTrue(memorySpace.isDropped());

    assertThat(this.dataset.getIndexing().getIndexes(), empty());

    for (final SovereignIndex<?> index : indexes) {
      assertFalse(index.isLive());
      assertNull(((SimpleIndex)index).getUnderlyingIndex());
    }

    @SuppressWarnings("unchecked")
    final Set<SovereignIndexMap<?, ?>> memorySpaceMapsDisposed = (Set<SovereignIndexMap<?, ?>>)memorySpaceMapsField.get(memorySpace);
     // unchecked
    assumeThat(memorySpaceMapsDisposed, isA(Set.class));
    for (final SovereignIndexMap<?, ?> underlyingIndex : underlyingIndexes) {
      try {
        underlyingIndex.estimateSize();   // Should fail if disposed
        fail();
      } catch (IllegalStateException e) {
        // Expected
      }
      assertThat(memorySpaceMapsDisposed, not(hasItem(underlyingIndex)));
    }
  }

  /**
   * Ensures that a disposed {@link com.terracottatech.sovereign.SovereignDataset SovereignDataset}
   * continues to have a non-failing response from {@code SovereignDataset.toString}
   */
  @Test
  public void testDisposedToString() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    assertNotNull(this.dataset.toString());
  }

  @Test
  public void testDisposedGetConfig() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    assertNotNull(((SovereignDatasetImpl) this.dataset).getConfig());
  }

  @Test
  public void testDisposedGetContainer() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    try {
      ((Catalog) this.dataset).getContainer();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedGetIndexing() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    assertNotNull(this.dataset.getIndexing());
  }

  @Test
  public void testDisposedGetPrimary() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    try {
      ((SovereignDatasetImpl) this.dataset).getPrimary();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedGetType() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    assertNotNull(((SovereignDatasetImpl) this.dataset).getType());
  }

  @Test
  public void testDisposedGetUsage() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    try {
      ((SovereignDatasetImpl) this.dataset).getUsage();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedAdd() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      this.dataset.add(IMMEDIATE, "monarch butterfly", TAXONOMIC_CLASS.newCell("insect"));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedRecords() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      this.dataset.records();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedDelete() throws Exception {
    final List<Record<String>> records;
    try (final Stream<Record<String>> recordStream = this.dataset.records()) {
      records = recordStream
          .filter(TAXONOMIC_CLASS.value().is("mammal"))
          .collect(toList());
    }
    assertThat(records, not(empty()));
    final Consumer<Record<String>> deleter = this.dataset.delete(IMMEDIATE);

    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      records.forEach(deleter);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedDelete1Arg() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      this.dataset.delete(IMMEDIATE, "echidna");
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedDelete2Arg() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      this.dataset.delete(IMMEDIATE, "echidna", TAXONOMIC_CLASS.value().is("mammal"));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedGet() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      this.dataset.get("echidna");
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedGetSortedIndexFor() throws Exception {
    AnimalsDataset.addIndexes(this.dataset);

    @SuppressWarnings("unchecked")
    final Catalog<String> catalog = (Catalog<String>) this.dataset;   // unchecked
    assertNotNull(catalog.getSortedIndexFor(TAXONOMIC_CLASS));

    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      catalog.getSortedIndexFor(TAXONOMIC_CLASS);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedHasSortedIndex() throws Exception {
    AnimalsDataset.addIndexes(this.dataset);

    @SuppressWarnings("unchecked")
    final Catalog<String> catalog = (Catalog<String>) this.dataset;   // unchecked
    assertTrue(catalog.hasSortedIndex(TAXONOMIC_CLASS));

    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      catalog.hasSortedIndex(TAXONOMIC_CLASS);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedApplyMutationFunction() throws Exception {
    final List<Record<String>> records;
    try (final Stream<Record<String>> recordStream = this.dataset.records()) {
      records = recordStream
          .filter(TAXONOMIC_CLASS.value().is("mammal"))
          .collect(toList());
    }
    assertThat(records, not(empty()));
    final Consumer<Record<String>> mutator =
        this.dataset.applyMutation(IMMEDIATE, alterRecord(assign(OBSERVATIONS, 1L)));

    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      records.forEach(mutator);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedApplyMutationKeyFunction() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      this.dataset.applyMutation(IMMEDIATE, "echidna", r -> true, alterRecord(assign(OBSERVATIONS, 1L)));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedApplyMutationFunctionBiFunction() throws Exception {
    final List<Record<String>> records;
    try (final Stream<Record<String>> recordStream = this.dataset.records()) {
      records = recordStream
          .filter(TAXONOMIC_CLASS.value().is("mammal"))
          .collect(toList());
    }
    assertThat(records, not(empty()));
    final Function<Record<String>, Optional<Object>> mutator =
        this.dataset.applyMutation(IMMEDIATE, alterRecord(assign(OBSERVATIONS, 1L)),
                                   (oldRecord, newRecord) -> {
              return Optional.empty();
            });

    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      records.stream().map(mutator).collect(toList());
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedApplyMutationKeyFunctionBiFunction() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      this.dataset.applyMutation(IMMEDIATE, "echidna", r -> true, alterRecord(assign(OBSERVATIONS, 1L)),
                                 (oldRecord, newRecord) -> Optional.empty());
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedLockingActionBegin() throws Exception {
    final InternalRecord<String> echidna = this.dataset.get("echidna");
    assertNotNull(echidna);

    ManagedAction<String> action = this.getLockingAction();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      action.begin(echidna);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedLockingActionEnd() throws Exception {
    InternalRecord<String> echidna = this.dataset.get("echidna");
    assertNotNull(echidna);

    ManagedAction<String> action = this.getLockingAction();

    try {
      echidna = action.begin(echidna);
      assertNotNull(echidna);
      this.dataset.getStorage().destroyDataSet(dataset.getUUID());
      action.end(echidna);
    } finally {
      closeManagedAction(action);
    }
  }

  @Test
  public void testDisposedLockingActionApply() throws Exception {
    InternalRecord<String> echidna = this.dataset.get("echidna");
    assertNotNull(echidna);

    ManagedAction<String> action = this.getLockingAction();
    @SuppressWarnings("unchecked")
    Function<Record<String>, ?> function =
        (Function<Record<String>, ?>) action;   // unchecked

    try {
      echidna = action.begin(echidna);
      assertNotNull(echidna);
      this.dataset.getStorage().destroyDataSet(dataset.getUUID());
      try {
        function.apply(echidna);
        fail();
      } catch (IllegalStateException e) {
        // Expected
      } finally {
        action.end(echidna);
      }
    } finally {
      this.closeManagedAction(action);
    }
  }

  @Test
  public void testDisposedLockingActionAccept() throws Exception {
    InternalRecord<String> echidna = this.dataset.get("echidna");
    assertNotNull(echidna);

    ManagedAction<String> action = this.getLockingAction();
    @SuppressWarnings("unchecked")
    Consumer<Record<String>> consumer =
        (Consumer<Record<String>>) action;   // unchecked

    try {
      echidna = action.begin(echidna);
      assertNotNull(echidna);
      this.dataset.getStorage().destroyDataSet(dataset.getUUID());
      try {
        consumer.accept(echidna);
        fail();
      } catch (IllegalStateException e) {
        // Expected
      } finally {
        action.end(echidna);
      }
    } finally {
      this.closeManagedAction(action);
    }
  }

  /**
   * Gets a {@link com.terracottatech.sovereign.impl.SovereignDatasetImpl.LockingAction LockingAction} to
   * use for disposal testing.
   *
   * @param <T> the {@code Function} return type
   * @param <C> the merged, visible types of {@code LockingAction}
   *
   * @return a "generic" {@code LockingAction} instance
   *
   * @throws Exception if {@code LockingAction} is not loadable or the obtained action does not conform
   *      to one of the types of {@code <C>}
   */
  private <T, C extends ManagedAction<String> & Consumer<Record<String>> & Function<Record<String>, T>>
  C getLockingAction() throws Exception {
    final Class<?> lockingActionClass = Class.forName(SovereignDatasetImpl.class.getName() + "$" + "LockingAction");

    @SuppressWarnings("unchecked")
    final C lockingAction = (C) this.dataset.delete(IMMEDIATE);   // unchecked
    assertThat(lockingAction, is(instanceOf(lockingActionClass)));
    assumeThat(lockingAction, isA(ManagedAction.class));
    assumeThat(lockingAction, isA(Consumer.class));
    assumeThat(lockingAction, isA(Function.class));

    return lockingAction;
  }

  /**
   * Ensures attempts to use a stream generated from a disposed dataset
   * are rudely interrupted.
   */
  @Test
  public void testStreamDispose() throws Exception {
    final Stream<Record<String>> animalStream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    dataset = null;

    // Note that continued addition of non-terminal operations to the Stream are *not* prevented
    final Stream<Record<String>> endangeredAnimalStream = animalStream
        .filter(r -> r.get(STATUS).isPresent() && r.get(STATUS).get().endsWith("endangered"));
    final Stream<Animal> mappedEndangeredAnimalStream = endangeredAnimalStream
        .map(Animal::new);

    try {
      mappedEndangeredAnimalStream.collect(toList());
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  /**
   * Ensures in-progress index creation is interrupted when the dataset is disposed.
   */
  @Test
  public void testDisposeDuringIndexing() throws Exception {
    final ExecutorService execService = Executors.newSingleThreadExecutor();

    /*
     * A semaphore for the test code to await the indexing of the second entry.
     */
    final Semaphore indexingSemaphore = new Semaphore(0);

    /*
     * A semaphore for the indexing code to await the disposal of the dataset.
     */
    final Semaphore disposeSemaphore = new Semaphore(0);

    /*
     * Replace the DataContainer in SovereignDataset.heap with a wrapped version that
     * calls SovereignDataset.dispose the 2nd time the DataContainer.get method is called.
     * This ensures the index creation has begun.
     */
    final Field datasetHeapField = dataset.getClass().getDeclaredField("heap");
    datasetHeapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final DataContainer<?, Locator, ?> heap = (DataContainer<?, Locator, ?>) datasetHeapField.get(dataset);
    final DataContainer<?, Locator, ?> spiedHeap = spy(heap);
    datasetHeapField.set(dataset, spiedHeap);
    doAnswer(
            Answers.CALLS_REAL_METHODS
    ).doAnswer(invocation -> {
      indexingSemaphore.release();
      Thread.yield();
      return invocation.callRealMethod();
    }).doAnswer(invocation -> {
      disposeSemaphore.tryAcquire(5, TimeUnit.SECONDS);
      return invocation.callRealMethod();
    }).when(spiedHeap).get(any(Locator.class));

    final Future<SovereignIndex<String>> indexFuture = execService.submit(dataset.getIndexing().createIndex(TAXONOMIC_CLASS, SovereignIndexSettings.btree()));

    assertTrue(indexingSemaphore.tryAcquire(20, TimeUnit.SECONDS));
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    disposeSemaphore.release();
    try {
      indexFuture.get();
      fail();
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof IllegalStateException)) {
        throw new AssertionError(e);
      }
    }

    assertNull(dataset.getIndexing().getIndex(TAXONOMIC_CLASS, SovereignIndexSettings.btree()));
  }

  @Ignore("Enable when attempting to reproduce a race in testDisposeDuringIndexing")
  @Test
  public void repeatTestDisposeDuringIndexing() throws Exception {
    for (int i = 0; i < 100; i++) {
      this.dataset = this.newDataset();
      testDisposeDuringIndexing();
    }
  }

  /* =====================================================================================================
   * The following test methods ensure <i>terminal</i> operations of a java.util.stream.Stream over
   * SovereignDatasetImpl throws an IllegalStateException once the dataset is disposed.
   */

  @Test
  public void testDisposedStreamForEachOperations() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.forEach(r -> {});
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamForEachOrdered() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.forEachOrdered(r -> {});
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamToArray() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.toArray();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamReduce() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.reduce((previous, current) -> current);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamCollect() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.collect(Collectors.counting());
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamMin() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.min((o1, o2) -> 0);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamMax() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.max((o1, o2) -> 0);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamCount() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.count();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamAnyMatch() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.anyMatch(r -> true);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamAllMatch() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.allMatch(r -> false);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamNoneMatch() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.noneMatch(r -> true);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamFindFirst() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.findFirst();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamFindAny() throws Exception {
    final Stream<Record<String>> stream = dataset.records();
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());

    try {
      stream.findAny();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedStreamIterator() throws Exception {
    try (final Stream<Record<String>> stream = dataset.records()) {
      final Iterator<Record<String>> iterator = stream.iterator();
      this.dataset.getStorage().destroyDataSet(dataset.getUUID());
      try {
        iterator.forEachRemaining(r -> {});
        fail();
      } catch (IllegalStateException e) {
        // Expected
      }
    }
  }

  @Test
  public void testDisposedStreamSpliterator() throws Exception {
    try (final Stream<Record<String>> stream = dataset.records()) {
      final Spliterator<Record<String>> spliterator = stream.spliterator();
      this.dataset.getStorage().destroyDataSet(dataset.getUUID());
      try {
        spliterator.forEachRemaining(r -> {});
        fail();
      } catch (IllegalStateException e) {
        // Expected
      }
    }
  }

  /**
   * This test attempts to ensure that a single index may be disposed without
   * undesired side-effect.
   */
  @Test
  public void testIndexDisposal() throws Exception {
    AnimalsDataset.addIndexes(this.dataset);

    final Space<?, ?> space = ((SovereignDatasetImpl<String>) this.dataset).getUsage();
    assumeThat(space, is(instanceOf(MemorySpace.class)));
    final MemorySpace memorySpace = (MemorySpace) space;
    assertFalse(memorySpace.isDropped());

    final Field memorySpaceMapsField = MemorySpace.class.getDeclaredField("maps");
    memorySpaceMapsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Set<SovereignIndexMap<?, ?>> memorySpaceMaps = (Set<SovereignIndexMap<?, ?>>)memorySpaceMapsField.get(memorySpace);    //
    // unchecked
    assumeThat(memorySpaceMaps, isA(Set.class));

    final SovereignIndex<String> statusIndex =
        this.dataset.getIndexing().getIndex(STATUS, SovereignIndexSettings.btree());
    assertNotNull(statusIndex);
    assertTrue(statusIndex.isLive());

    assumeThat(statusIndex, is(instanceOf(SimpleIndex.class)));
    final SovereignIndexMap<?, ?> underlyingStatusIndex = ((SimpleIndex)statusIndex).getUnderlyingIndex();
    underlyingStatusIndex.estimateSize();   // Will fail if dropped

    assertThat(memorySpaceMaps, hasItem(underlyingStatusIndex));

    final SovereignIndex<String> taxonomicIndex =
        this.dataset.getIndexing().getIndex(TAXONOMIC_CLASS, SovereignIndexSettings.btree());
    assertNotNull(taxonomicIndex);
    assertTrue(taxonomicIndex.isLive());

    final List<Animal> fullExpected;
    try (final Stream<Record<String>> recordStream = this.dataset.records()) {
      fullExpected = recordStream
          .map(Animal::new)
          .collect(toList());
    }

    /*
     * As of 6 March 2015, the use of com.terracottatech.store.functions.CellFunctions.compare in
     * filter will result in use of a secondary index for the cell (if defined).  However, beginning
     * the filter with will com.terracottatech.store.functions.CellFunctions.hasCell will *prevent*
     * the use of an index.
     */
    final List<Animal> animalsWithStatusUsingIndex;
    try (final Stream<Record<String>> recordStream = this.dataset.records()) {
      animalsWithStatusUsingIndex = recordStream
          .filter(STATUS.value().isGreaterThan(""))
          .map(Animal::new)
          .collect(toList());
    }
    compareWithDirectIndex(STATUS, animalsWithStatusUsingIndex.stream());

    /*
     * Ensure index returns same records as comparison scan
     */
    final List<Animal> animalsWithStatusNoIndex;
    try (final Stream<Record<String>> recordStream = this.dataset.records()) {
      animalsWithStatusNoIndex = recordStream
          .filter(r -> r.get(STATUS).isPresent())
          .map(Animal::new)
          .collect(toList());
    }
    compareWithDirectIndex(STATUS, animalsWithStatusNoIndex.stream());

    final List<Animal> animalsWithClassNoIndex;
    try (final Stream<Record<String>> recordStream = this.dataset.records()) {
      animalsWithClassNoIndex = recordStream
          .filter(r -> r.get(TAXONOMIC_CLASS).isPresent())
          .map(Animal::new)
          .collect(toList());
    }
    compareWithDirectIndex(TAXONOMIC_CLASS, animalsWithClassNoIndex.stream());

    /*
     * Now discard the status index.
     */
    this.dataset.getIndexing().destroyIndex(statusIndex);
    assertFalse(statusIndex.isLive());
    assertNull(this.dataset.getIndexing().getIndex(STATUS, SovereignIndexSettings.btree()));

    assertFalse(this.dataset.isDisposed());
    assertTrue(taxonomicIndex.isLive());

    /*
     * Ensure the status index references are discarded
     */
    try {
      underlyingStatusIndex.estimateSize();   // Will fail if dropped
      fail();
    } catch (Exception e) {
      // Expected
    }

    assertFalse(memorySpace.isDropped());
    assertThat(memorySpaceMaps, not(hasItem(underlyingStatusIndex)));

    /*
     * Ensure stream operations remain fully-functional after discarding status index
     */
    try (final Stream<Record<String>> recordStream = this.dataset.records()) {
      compareStreams(fullExpected.stream(), recordStream.map(Animal::new));
    }

    try (final Stream<Record<String>> recordStream = this.dataset.records()) {
      final Stream<Animal> animalsWithStatusDroppedIndex = recordStream
          .filter(STATUS.value().isGreaterThan(""))
          .map(Animal::new);
      compareStreams(animalsWithStatusUsingIndex.stream(), animalsWithStatusDroppedIndex);
    }
  }

  /**
   * This test exercises index creation and disposal by repeatedly creating and disposing
   * of the same set of indexes.  This test does not dispose of the SovereignDataset.
   */
  // TODO: Determine why the Space stats do not change as expected
  @Test
  public void testRepeatedIndexDisposal() throws Exception {

    final AtomicLong serial = new AtomicLong(0);
    final CellDefinition<Long> serialNumber = CellDefinition.define("serialNumber", Type.LONG);
    final Consumer<Record<String>> setSerialNumber =
        this.dataset.applyMutation(IMMEDIATE, alterRecord(assign(serialNumber, serial.getAndIncrement())));
    try (final Stream<Record<String>> recordStream = this.dataset.records()) {
      recordStream.forEach(setSerialNumber);
    }

    // Attempt to clean out storage
    for (int i = 0; i < 10; i++) {
      System.gc();
      System.runFinalization();
    }

    final Space<?, ?> space = ((SovereignDatasetImpl)this.dataset).getUsage();
    long initialSpaceUsed = space.getUsed();
    long maxSpaceUsed = initialSpaceUsed;
    long maxSpaceCapacity = space.getCapacity();
    long maxSpaceReserved = space.getReserved();

    final BufferPoolMXBean bufferPoolMXBean = this.getDirectStatsBean();
    long maxBufferUsed = bufferPoolMXBean.getMemoryUsed();
    long maxBufferCapacity = bufferPoolMXBean.getTotalCapacity();
    long initialBufferCount = bufferPoolMXBean.getCount();
    long maxBufferCount = initialBufferCount;

    long maxCommittedVMSize = PhysicalMemory.ourCommittedVirtualMemory();
    long maxPhysicalUsed = PhysicalMemory.totalPhysicalMemory() - PhysicalMemory.freePhysicalMemory();
    long maxSwapUsed = PhysicalMemory.totalSwapSpace() - PhysicalMemory.freeSwapSpace();

    System.out.println("Before test stats:");
    System.out.format("    Space: used=%d capacity=%d reserved=%d%n", maxSpaceUsed, maxSpaceCapacity, maxSpaceReserved);
    System.out.format("    Buffer stats: count=%d used=%d capacity=%d%n",
        maxBufferCount, maxBufferUsed, maxBufferCapacity);
    System.out.format("    Memory stats: committed=%d, swap=%d/%d, physical=%d/%d%n",
        maxCommittedVMSize, maxSwapUsed, PhysicalMemory.totalSwapSpace(), maxPhysicalUsed, PhysicalMemory.totalPhysicalMemory());

    final SovereignIndexing indexing = this.dataset.getIndexing();
    for (int i = 0; i < 100; i++) {
      final SovereignIndex<String> taxonomicClassIndex = indexing.createIndex(TAXONOMIC_CLASS, SovereignIndexSettings.btree()).call();
      final SovereignIndex<String> statusIndex = indexing.createIndex(STATUS, SovereignIndexSettings.btree()).call();
      final SovereignIndex<Long> serialNumberIndex = indexing.createIndex(serialNumber, SovereignIndexSettings.btree())
        .call();

      assertThat(getIndexes(indexing), hasItem(taxonomicClassIndex));
      assertTrue(taxonomicClassIndex.isLive());
      assertThat(getIndexes(indexing), hasItem(statusIndex));
      assertTrue(statusIndex.isLive());
      assertThat(getIndexes(indexing), hasItem(serialNumberIndex));
      assertTrue(serialNumberIndex.isLive());

      maxSpaceUsed = Math.max(maxSpaceUsed, space.getUsed());
      maxSpaceCapacity = Math.max(maxSpaceCapacity, space.getCapacity());
      maxSpaceReserved = Math.max(maxSpaceReserved, space.getReserved());

      maxBufferUsed = Math.max(maxBufferUsed, bufferPoolMXBean.getMemoryUsed());
      maxBufferCapacity = Math.max(maxBufferCapacity, bufferPoolMXBean.getTotalCapacity());
      maxBufferCount = Math.max(maxBufferCount, bufferPoolMXBean.getCount());

      maxCommittedVMSize = Math.max(maxCommittedVMSize, PhysicalMemory.ourCommittedVirtualMemory());
      maxPhysicalUsed = Math.max(maxPhysicalUsed, PhysicalMemory.totalPhysicalMemory() - PhysicalMemory.freePhysicalMemory());
      maxSwapUsed = Math.max(maxSwapUsed, PhysicalMemory.totalSwapSpace() - PhysicalMemory.freeSwapSpace());

      indexing.destroyIndex(taxonomicClassIndex);
      assertFalse(taxonomicClassIndex.isLive());
      indexing.destroyIndex(statusIndex);
      assertFalse(statusIndex.isLive());
      indexing.destroyIndex(serialNumberIndex);
      assertFalse(serialNumberIndex.isLive());

      assertThat(getIndexes(indexing), not(hasItem(taxonomicClassIndex)));
      assertThat(getIndexes(indexing), not(hasItem(statusIndex)));
      assertThat(getIndexes(indexing), not(hasItem(serialNumberIndex)));
    }
    System.out.println("Test consumption stats:");
    System.out.format("    Space: used=%d capacity=%d reserved=%d%n", maxSpaceUsed, maxSpaceCapacity, maxSpaceReserved);
    System.out.format("    Buffer stats: count=%d maxSpaceUsed=%d maxCapacity=%d%n",
        maxBufferCount, maxBufferUsed, maxBufferCapacity);
    System.out.format("    Memory stats: maxCommitted=%d, maxSwap=%d/%d, maxPhysical=%d/%d%n",
        maxCommittedVMSize, maxSwapUsed, PhysicalMemory.totalSwapSpace(), maxPhysicalUsed, PhysicalMemory.totalPhysicalMemory());

    // Attempt to clean out storage
    for (int i = 0; i < 20; i++) {
      System.gc();
      System.runFinalization();
      Thread.yield();
    }

    System.out.println("Post test stats:");
    long lastBufferCount = bufferPoolMXBean.getCount();
    long lastSpaceUsed = space.getUsed();
    System.out.format("    Space: used=%d capacity=%d reserved=%d%n",
        lastSpaceUsed, space.getCapacity(), space.getReserved());
    System.out.format("    Buffer stats: count=%d used=%d capacity=%d%n",
        lastBufferCount, bufferPoolMXBean.getMemoryUsed(), bufferPoolMXBean.getTotalCapacity());
    System.out.format("    Memory stats: committed=%d, swap=%d/%d, physical=%d/%d%n",
        PhysicalMemory.ourCommittedVirtualMemory(),
        PhysicalMemory.totalSwapSpace() - PhysicalMemory.freeSwapSpace(), PhysicalMemory.totalSwapSpace(),
        PhysicalMemory.totalPhysicalMemory() - PhysicalMemory.freePhysicalMemory(), PhysicalMemory.totalPhysicalMemory());

    //Diagnostics.dumpHeap(true, "e:\\dump");
    assertThatEventually(space::getUsed, is(initialSpaceUsed)).and(bufferPoolMXBean::getCount, is(initialBufferCount))
            .within(Duration.ofSeconds(30));
  }

  @SuppressWarnings("unchecked")
  private List<SovereignIndex<?>> getIndexes(SovereignIndexing indexing) {
    return indexing.getIndexes();
  }

  /**
   * Compares the contents of two {@link java.util.stream.Stream Stream} instances.  The items
   * returned by each {@code Stream} are compared using {@link Object#equals(Object) equals} in
   * the order the items are presented by the {@code Stream}.
   *
   * @param s1 the first {@code Stream} to compare
   * @param s2 the second {@code Stream} to compare
   */
  private void compareStreams(final Stream<Animal> s1, final Stream<Animal> s2) {
    final Iterator<Animal> i1 = s1.sorted((a1, a2) -> a1.getName().compareTo(a2.getName())).iterator();
    final Iterator<Animal> i2 = s2.sorted((a1, a2) -> a1.getName().compareTo(a2.getName())).iterator();
    while (i1.hasNext() && i2.hasNext()) {
      assertEquals(i1.next(), i2.next());
    }
    assertEquals(i1.hasNext(), i2.hasNext());
  }

  /**
   * Compares the items accessible through the index defined in {@link #dataset} over {@code indexedCellDefinition}
   * with the content of the {@link java.util.stream.Stream Stream} {@code originalAnimals}.  The
   * index and stream content are sorted before being compared.
   *
   * @param indexedCellDefinition the {@code CellDefinition} of the indexed cell
   * @param originalAnimals a {@code Stream} providing the expected animals
   * @param <T> the value type of {@code indexedCellDefinition}
   *
   * @see #streamUsingIndex(SovereignDatasetImpl, CellDefinition)
   */
  private <T extends Comparable<T>> void compareWithDirectIndex(final CellDefinition<T> indexedCellDefinition, final Stream<Animal> originalAnimals) {
    final List<Animal> scannedEntries = originalAnimals
        .sorted((a1, a2) -> a1.getName().compareTo(a2.getName()))
        .collect(toList());
    final List<Animal> indexEntries = streamUsingIndex((SovereignDatasetImpl<String>)this.dataset, indexedCellDefinition)
        .sorted((r1, r2) -> r1.getKey().compareTo(r2.getKey()))
        .map(Animal::new)
        .collect(toList());

    assertEquals(scannedEntries.size(), indexEntries.size());
    for (int i = 0; i < scannedEntries.size(); i++) {
      assertEquals(String.format("[%d]", i), scannedEntries.get(i), indexEntries.get(i));
    }
  }

  /**
   * This test attempts to push allocation/deallocation to catch lingering storage
   * references.
   */
  @Test
  public void testStorageStress() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    this.dataset = null;

    // Attempt to clean out storage
    for (int i = 0; i < 10; i++) {
      System.gc();
      System.runFinalization();
    }


    final BufferPoolMXBean bufferPoolMXBean = this.getDirectStatsBean();
    long maxBufferUsed = bufferPoolMXBean.getMemoryUsed();
    long maxBufferCapacity = bufferPoolMXBean.getTotalCapacity();
    long initialBufferCount = bufferPoolMXBean.getCount();
    long maxBufferCount = initialBufferCount;

    long maxCommittedVMSize = PhysicalMemory.ourCommittedVirtualMemory();
    long maxPhysicalUsed = PhysicalMemory.totalPhysicalMemory() - PhysicalMemory.freePhysicalMemory();
    long maxSwapUsed = PhysicalMemory.totalSwapSpace() - PhysicalMemory.freeSwapSpace();

    System.out.println("Before test stats:");
    System.out.format("    Buffer stats: count=%d used=%d capacity=%d%n",
        maxBufferCount, maxBufferUsed, maxBufferCapacity);
    System.out.format("    Memory stats: committed=%d, swap=%d/%d, physical=%d/%d%n",
        maxCommittedVMSize, maxSwapUsed, PhysicalMemory.totalSwapSpace(), maxPhysicalUsed, PhysicalMemory.totalPhysicalMemory());

    int lastObservedCount = -1;
    for (int i = 0; i < 10; i++) {
      final SovereignDataset<String> localDataset = this.newDataset();

      SovereignAllocationResource alloc = ((SovereignDatasetImpl<String>) localDataset).getRuntime().allocator();
      SovereignStorage<?, ?> storage = ((SovereignDatasetImpl<String>) localDataset).getStorage();
      System.out.println("Init: " + storage.getBufferResource());
      System.out.println("Init: " + alloc.toString());

      AnimalsDataset.addIndexes(localDataset);

      final List<Animal> endangeredAnimals;
      try (final Stream<Animal> mappedEndangeredAnimalStream = localDataset.records()
        .filter(r -> r.get(STATUS).isPresent() && r.get(STATUS).get().endsWith("endangered"))
        .map(Animal::new)) {
        endangeredAnimals = mappedEndangeredAnimalStream.collect(toList());

        maxBufferUsed = Math.max(maxBufferUsed, bufferPoolMXBean.getMemoryUsed());
        maxBufferCapacity = Math.max(maxBufferCapacity, bufferPoolMXBean.getTotalCapacity());
        maxBufferCount = Math.max(maxBufferCount, bufferPoolMXBean.getCount());
      }

      maxCommittedVMSize = Math.max(maxCommittedVMSize, PhysicalMemory.ourCommittedVirtualMemory());
      maxPhysicalUsed = Math.max(maxPhysicalUsed, PhysicalMemory.totalPhysicalMemory() - PhysicalMemory.freePhysicalMemory());
      maxSwapUsed = Math.max(maxSwapUsed, PhysicalMemory.totalSwapSpace() - PhysicalMemory.freeSwapSpace());

      if (lastObservedCount != -1) {
        assertThat(endangeredAnimals, hasSize(lastObservedCount));
      }
      lastObservedCount = endangeredAnimals.size();
      System.out.println("Before: " + storage.getBufferResource());
      System.out.println("Before: " + alloc.toString());
      localDataset.getStorage().destroyDataSet(localDataset.getUUID());

      // interestingly, without this, it sometimes fails to reclaim offheap
      // data on several machines I tests
      for (int j = 0; j < 10; j++) {
        System.gc();
        System.runFinalization();
      }
      System.out.println("After: " + storage.getBufferResource());
      System.out.println("After: " + alloc.toString());
      assertThatEventually(alloc::allocatedBytes, is(0l)).within(Duration.ofSeconds(30));
    }

    System.out.println("Test consumption stats:");
    System.out.format("    Buffer stats: count=%d maxUsed=%d maxCapacity=%d%n",
        maxBufferCount, maxBufferUsed, maxBufferCapacity);
    System.out.format("    Memory stats: maxCommitted=%d, maxSwap=%d/%d, maxPhysical=%d/%d%n",
        maxCommittedVMSize, maxSwapUsed, PhysicalMemory.totalSwapSpace(), maxPhysicalUsed, PhysicalMemory.totalPhysicalMemory());

    // Attempt to clean out storage
    for (int i = 0; i < 10; i++) {
      System.gc();
      System.runFinalization();
      Thread.yield();
    }

    System.out.println("Post test stats:");
    long lastBufferCount = bufferPoolMXBean.getCount();
    System.out.format("    Buffer stats: count=%d used=%d capacity=%d%n",
        lastBufferCount, bufferPoolMXBean.getMemoryUsed(), bufferPoolMXBean.getTotalCapacity());
    System.out.format("    Memory stats: committed=%d, swap=%d/%d, physical=%d/%d%n",
        PhysicalMemory.ourCommittedVirtualMemory(),
        PhysicalMemory.totalSwapSpace() - PhysicalMemory.freeSwapSpace(), PhysicalMemory.totalSwapSpace(),
        PhysicalMemory.totalPhysicalMemory() - PhysicalMemory.freePhysicalMemory(), PhysicalMemory.totalPhysicalMemory());

    assertThatEventually(bufferPoolMXBean::getCount, lessThanOrEqualTo(initialBufferCount)).within(Duration.ofSeconds(30));
  }

  private void closeManagedAction(final ManagedAction<String> action) {
    if (action instanceof AutoCloseable) {
      try {
        ((AutoCloseable)action).close();
      } catch (Exception e) {
        // Ignored
      }
    }
  }

  private volatile BufferPoolMXBean directStatsBean;

  /**
   * Gets the {@link java.lang.management.BufferPoolMXBean BufferPoolMXBean} for the {@code direct}
   * buffer pool (the pool from which off-heap requests are satisfied).
   *
   * @return the {@code BufferPoolMXBean} for the {@code direct} pool
   */
  private BufferPoolMXBean getDirectStatsBean() {
    BufferPoolMXBean directStatsBean = this.directStatsBean;
    if (directStatsBean == null) {
      this.directStatsBean = directStatsBean = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
          .filter(pool -> pool.getName().equals("direct")).findAny().get();
    }
    return directStatsBean;
  }

  /**
   * Obtains a {@code Stream} over a {@link com.terracottatech.sovereign.SovereignDataset SovereignDataset}
   * using defined secondary index.
   *
   * @param dataset the {@code SovereignDataset} over which the {@code Stream} is defined
   * @param indexedCell the {@code CellDefinition} for the index to use
   * @param <K> the type of the dataset key
   * @param <V> the value type of the cell
   *
   * @return a new {@code Stream} returning records from {@code dataset} available through the index over
   *      {@code indexedCell}
   */
  private static <K extends Comparable<K>, V extends Comparable<V>>
  Stream<Record<K>> streamUsingIndex(final SovereignDatasetImpl<K> dataset, final CellDefinition<V> indexedCell) {
    final IndexSpliterator<K, V, SovereignDatasetImpl<K>> spliterator = new IndexSpliterator<>(dataset, indexedCell);
    final Stream<Record<K>> stream = StreamSupport.stream(spliterator, false);
    stream.onClose(spliterator::close);
    return stream;
  }

  /**
   * Defines a {@code Spliterator} which supplies {@link com.terracottatech.store.Record Record}
   * instances from a {@link com.terracottatech.sovereign.SovereignDataset SovereignDataset} ordered
   * by a defined secondary index.
   * <p/>
   * An instance of this {@code Spliterator} is <b>not</b> thread-safe; it must be used from a
   * single thread.
   * <p/>
   * This {@code Spliterator} opens a {@link com.terracottatech.sovereign.spi.store.Context Context} during
   * retrieval of the first record and closes the {@code Context} once the last record in the sequence is
   * fetched.
   *
   * @param <K> the type of the primary key for the {@code SovereignDataset}
   * @param <V> the value type of the index to use
   * @param <D> the {@code SovereignDataset} type
   */
  private static final class IndexSpliterator<K extends Comparable<K>, V extends Comparable<V>,
      D extends SovereignDataset<K> & Catalog<K>>
      implements Spliterator<Record<K>>, AutoCloseable {

    private final SovereignContainer<K> dataContainer;
    private final SovereignSortedIndexMap<V, K> index;

    /**
     * The {@code Context} for this {@code Spliterator} once opened.  Use
     * {@link #getContext()} to access.
     */
    private volatile ContextImpl context;

    /**
     * The {@code Locator} for the last served record.  If {@code null},
     * iteration has not begun.
     */
    private volatile PersistentMemoryLocator current;

    /**
     * Create an {@code IndexSpliterator} over the {@link CellDefinition CellDefinition}
     * in the {@link com.terracottatech.sovereign.SovereignDataset SovereignDataset} provided.
     *
     * @param dataset the {@code SovereignDataset} for which the {@code Spliterator} is created
     * @param indexedCellDefinition the {@code CellDefinition} for the index to use
     */
    public IndexSpliterator(final D dataset, final CellDefinition<V> indexedCellDefinition) {
      Objects.requireNonNull(dataset, "dataset");
      Objects.requireNonNull(indexedCellDefinition, "indexedCellDefinition");

      this.dataContainer = dataset.getContainer();
      this.index = dataset.getSortedIndexFor(indexedCellDefinition);
      if (this.index == null) {
        throw new IllegalArgumentException("No index for cell '" + indexedCellDefinition.name() + "'");
      }
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void finalize() throws Throwable {
      this.close();
      super.finalize();
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Record<K>> action) {
      Objects.requireNonNull(action, "action");

      PersistentMemoryLocator current = this.next();
      if (current.isEndpoint()) {
        return false;
      }

      Record<K> p = this.dataContainer.get(current);
      action.accept(p);
      return true;
    }

    /**
     * Gets the {@link com.terracottatech.sovereign.spi.store.Locator Locator} for the
     * first/next {@link com.terracottatech.store.Record Record} to present using
     * the index.
     *
     * @return the {@code Locator} for the next {@code Record}; if at the end of the
     *    index, {@link PersistentMemoryLocator#INVALID} is returned.
     */
    private PersistentMemoryLocator next() {
      PersistentMemoryLocator current = this.current;
      if (current == null) {
        current = this.current = this.index.first(this.getContext());
      } else if (current.isEndpoint()) {
        return current;     // Sequence was exhausted by a prior call
      } else {
        current = this.current = current.next();
      }

      /*
       * Have first/next Locator here; see if the sequence is exhausted.  If so,
       * close the Context
       */
      if (current.isEndpoint()) {
        if (this.context != null) {
          this.context.close();
          this.context = null;
        }
        current = this.current = PersistentMemoryLocator.INVALID;
      }

      return current;
    }

    /**
     * Gets the {@link com.terracottatech.sovereign.spi.store.Context Context} to use for
     * traversing the index.  This method starts a new {@code Context} the first time it
     * is called.
     *
     * @return the {@code Context} to use for traversing the index
     */
    private ContextImpl getContext() {
      ContextImpl context = this.context;
      if (context == null) {
        context = this.context = this.dataContainer.start(true);
      }
      return context;
    }

    /**
     * Closes this {@code Spliterator}.
     */
    @Override
    public void close() {
      this.current = PersistentMemoryLocator.INVALID;
      try {
        final Context context = this.context;
        if (context != null) {
          this.context = null;
          context.close();
        }
      } catch (Exception e) {
        // Ignored
      }
    }

    @Override
    public Spliterator<Record<K>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return 0;
    }
  }
}
