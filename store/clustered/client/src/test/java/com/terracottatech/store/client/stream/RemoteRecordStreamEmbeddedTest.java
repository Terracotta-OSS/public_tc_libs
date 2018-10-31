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

import com.terracottatech.store.Cell;
import com.terracottatech.store.CellCollection;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Record;
import com.terracottatech.store.common.dataset.stream.PackageSupport;
import com.terracottatech.store.common.dataset.stream.PipelineMetaData;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;
import com.terracottatech.store.common.dataset.stream.WrappedStream;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.stream.RecordStream;
import com.terracottatech.test.data.Animals.Animal;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.terracottatech.store.client.stream.AbstractRemoteStreamTest.isOrderedAccordingTo;
import static com.terracottatech.test.data.Animals.ANIMALS;
import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static java.util.Comparator.comparing;
import static java.util.Comparator.nullsFirst;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Performs basic tests against {@link RemoteRecordStream}.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class RemoteRecordStreamEmbeddedTest {

  @Rule
  public EmbeddedAnimalDataset animalDataset = new EmbeddedAnimalDataset();

  @Test
  public void testSetTerminalAction() throws Exception {
    tryStream((recordStream) -> {
      // Force creation of a native Java stream by appending a non-portable operation
      Stream<Record<String>> stream = recordStream.peek((record) -> { });

      final AtomicMarkableReference<PipelineMetaData> passedMetaData = new AtomicMarkableReference<>(null, false);
      final Consumer<PipelineMetaData> firstAction = metaData -> passedMetaData.set(metaData, false);
      /*
       * Using setPipelineConsumer *without* preserving the terminal action set by
       * RootRemoteRecordStream.nonMutableStream will cause a terminal action failure!
       */
      PackageSupport.setPipelineConsumer(recordStream.getRootWrappedStream().getMetaData(), firstAction);
      assertThat(recordStream.getRootWrappedStream().getTerminalAction(), is(sameInstance(firstAction)));
      assertThat(recordStream.getRootWrappedStream().getMetaData().getPipelineConsumer(), is(sameInstance(firstAction)));
      assertThat(passedMetaData.getReference(), is(nullValue()));

      final Consumer<PipelineMetaData> secondAction = metaData -> passedMetaData.attemptMark(metaData, true);
      recordStream.getRootWrappedStream().appendTerminalAction(secondAction);

      try {
        stream.count();
        fail();
      } catch (IllegalStateException e) {
        assertThat(e.getMessage(), is(equalTo("terminal operation not appended")));
      }
    });
  }

  @SuppressWarnings("Duplicates")
  @Test
  public void testAppendTerminalAction() throws Exception {
    tryStream((recordStream) -> {
      // Force creation of a native Java stream by appending a non-portable operation
      Stream<Record<String>> stream = recordStream.peek((record) -> { });

      final Consumer<PipelineMetaData> firstAction = recordStream.getRootWrappedStream().getTerminalAction();
      assertThat(firstAction, is(notNullValue()));

      final AtomicMarkableReference<PipelineMetaData> passedMetaData = new AtomicMarkableReference<>(null, false);
      final Consumer<PipelineMetaData> secondAction = metaData -> passedMetaData.set(metaData, false);
      recordStream.getRootWrappedStream().appendTerminalAction(secondAction);
      assertThat(passedMetaData.getReference(), is(nullValue()));

      final Consumer<PipelineMetaData> thirdAction = metaData -> passedMetaData.attemptMark(metaData, true);
      recordStream.getRootWrappedStream().appendTerminalAction(thirdAction);

      assertThat(stream.count(), is(not(0L)));

      final boolean[] mark = new boolean[1];
      assertThat(passedMetaData.get(mark), is(sameInstance(recordStream.getRootWrappedStream().getMetaData())));
      assertThat(mark[0], is(true));
    });
  }

  @Test
  public void testIterator() throws Exception {
    tryStream((stream) -> {
      Iterator<Record<String>> iterator = stream.iterator();
      assertPortableOps(stream);
      assertNonPortableOps(stream, TerminalOperation.ITERATOR);
      assertTerminalPortableOp(stream, null);
      assertContainsInAnyOrder(iterator, ANIMALS.iterator());
    });
  }

  @Test
  public void testSpliterator() throws Exception {
    tryStream((stream) -> {
      Spliterator<Record<String>> spliterator = stream.spliterator();
      assertPortableOps(stream);
      assertNonPortableOps(stream, TerminalOperation.SPLITERATOR);
      assertTerminalPortableOp(stream, null);
      assertContainsInAnyOrder(spliterator, ANIMALS.iterator());
    });
  }

  @Test
  public void testIsParallel() throws Exception {
    tryStream(stream -> {
      assertFalse(stream.isParallel());
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testSequential() throws Exception {
    tryStream(recordStream -> {
      Stream<Record<String>> stream = recordStream.parallel();
      assertThat(stream, is(instanceOf(RemoteReferenceStream.class)));
      assertThat(stream.isParallel(), is(true));
      assertThat(stream.count(), is(ANIMALS.stream().parallel().count()));
      assertPortableOps(recordStream, IntermediateOperation.PARALLEL);
      assertTerminalPortableOp(recordStream, TerminalOperation.COUNT);
      assertNonPortableOps(recordStream);
    });
  }

  @Test
  public void testParallel() throws Exception {
    tryStream(recordStream -> {
      Stream<Record<String>> stream = recordStream.sequential();
      assertThat(stream, is(instanceOf(RemoteReferenceStream.class)));
      assertThat(stream.isParallel(), is(false));
      assertThat(stream.count(), is(ANIMALS.stream().sequential().count()));
      assertPortableOps(recordStream, IntermediateOperation.SEQUENTIAL);
      assertTerminalPortableOp(recordStream, TerminalOperation.COUNT);
      assertNonPortableOps(recordStream);
    });
  }

  @Test
  public void testUnordered() throws Exception {
    Comparator<Record<?>> comparator = TAXONOMIC_CLASS.valueOr("").asComparator();
    tryStream(stream -> {
      Stream<Record<String>> objStream = stream.unordered();
      assertThat(objStream, is(instanceOf(RemoteReferenceStream.class)));
      assertFalse(stream.getRootStream().isStreamOrdered());

      Stream<Record<String>> sortedObjStream = objStream.sorted(comparator);
      assertTrue(stream.getRootStream().isStreamOrdered());

      List<Animal> orderedList = sortedObjStream.map(Animal::new).collect(toList());

      assertPortableOps(stream, IntermediateOperation.UNORDERED, IntermediateOperation.SORTED_1);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.MAP, TerminalOperation.COLLECT_1);
      assertThat(orderedList, isOrderedAccordingTo(nullsFirst(comparing(Animal::getTaxonomicClass))));
    });
  }

  @SuppressWarnings({"Duplicates", "try"})
  @Test
  public void testOnCloseRoot() throws Exception {
    try (RemoteRecordStream<String> testStream = getTestStream()) {
      AtomicBoolean closeCalled = new AtomicBoolean(false);
      assertThat(testStream.onClose(() -> closeCalled.set(true)), is(sameInstance(testStream)));

      // Add portable intermediate operation to test for proper onClose chaining
      Stream<Record<String>> distinctStream = testStream.distinct();
      assertThat(distinctStream, is(sameInstance(testStream)));
      AtomicBoolean distinctStreamClosed = new AtomicBoolean(false);
      assertThat(distinctStream.onClose(() -> distinctStreamClosed.set(true)), is(sameInstance(distinctStream)));

      // Add non-portable intermediate operation to test for proper onClose chaining
      Stream<Animal> animalStream = distinctStream.map(Animal::new);
      AtomicBoolean animalStreamClosed = new AtomicBoolean(false);
      assertThat(animalStream.onClose(() -> animalStreamClosed.set(true)), is(sameInstance(animalStream)));

      testStream.close();       // Invoke close on root segment
      assertTrue(closeCalled.get());
      assertTrue(distinctStreamClosed.get());
      assertTrue(animalStreamClosed.get());
    }
  }

  @SuppressWarnings("Duplicates")
  @Test
  public void testOnClosePortable() throws Exception {
    try (RemoteRecordStream<String> testStream = getTestStream()) {
      AtomicBoolean closeCalled = new AtomicBoolean(false);
      assertThat(testStream.onClose(() -> closeCalled.set(true)), is(sameInstance(testStream)));

      // Add portable intermediate operation to test for proper onClose chaining
      Stream<Record<String>> distinctStream = testStream.distinct();
      assertThat(distinctStream, is(sameInstance(testStream)));
      AtomicBoolean distinctStreamClosed = new AtomicBoolean(false);
      assertThat(distinctStream.onClose(() -> distinctStreamClosed.set(true)), is(sameInstance(distinctStream)));

      // Add non-portable intermediate operation to test for proper onClose chaining
      Stream<Animal> animalStream = distinctStream.map(Animal::new);
      AtomicBoolean animalStreamClosed = new AtomicBoolean(false);
      assertThat(animalStream.onClose(() -> animalStreamClosed.set(true)), is(sameInstance(animalStream)));

      distinctStream.close();     // Invoke close on portable pipeline segment
      assertTrue(closeCalled.get());
      assertTrue(distinctStreamClosed.get());
      assertTrue(animalStreamClosed.get());
    }
  }

  @SuppressWarnings("Duplicates")
  @Test
  public void testOnCloseNonPortable() throws Exception {
    RemoteRecordStream<String> testStream = getTestStream();
    try {
      AtomicBoolean closeCalled = new AtomicBoolean(false);
      assertThat(testStream.onClose(() -> closeCalled.set(true)), is(sameInstance(testStream)));

      // Add portable intermediate operation to test for proper onClose chaining
      Stream<Record<String>> distinctStream = testStream.distinct();
      assertThat(distinctStream, is(sameInstance(testStream)));
      AtomicBoolean distinctStreamClosed = new AtomicBoolean(false);
      assertThat(distinctStream.onClose(() -> distinctStreamClosed.set(true)), is(sameInstance(distinctStream)));

      // Add non-portable intermediate operation to test for proper onClose chaining
      Stream<Animal> animalStream = distinctStream.map(Animal::new);
      AtomicBoolean animalStreamClosed = new AtomicBoolean(false);
      assertThat(animalStream.onClose(() -> animalStreamClosed.set(true)), is(sameInstance(animalStream)));

      animalStream.close();     // Invoke close on non-portable pipeline segment
      assertTrue(closeCalled.get());
      assertTrue(distinctStreamClosed.get());
      assertTrue(animalStreamClosed.get());
    } finally {
      if (testStream != null) {
        testStream.close();
      }
    }
  }

  @Test
  public void testFilterNonPortable() throws Exception {
    // Non-portable Predicate
    tryStream(stream -> {
      Stream<Record<String>> targetStream = stream.filter(r -> r.getKey().equals("echidna"));
      assertThat(targetStream, is(instanceOf(RecordStream.class)));
      Iterator<Record<String>> echidnas = targetStream.iterator();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.FILTER, TerminalOperation.ITERATOR);
      assertContainsInAnyOrder(echidnas, ANIMALS.stream().filter(a -> a.getName().equals("echidna")).iterator());
    });
  }

  @Test
  public void testFilterPortable() throws Exception {
    // Portable Predicate
    tryStream(stream -> {
      Stream<Record<String>> targetStream = stream.filter(TAXONOMIC_CLASS.value().is("mammal"));
      assertThat(targetStream, is(sameInstance(stream)));
      Iterator<Record<String>> mammals = targetStream.iterator();
      assertPortableOps(stream, IntermediateOperation.FILTER);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.ITERATOR);
      assertContainsInAnyOrder(mammals, ANIMALS.stream().filter(a -> a.getTaxonomicClass().equals("mammal")).iterator());
    });
  }

  @Test
  public void testMapNonPortable() throws Exception {
    tryStream(stream -> {
      Stream<String> targetStream = stream.map(r -> r.get(TAXONOMIC_CLASS).orElse("unknown"));
      assertThat(targetStream, is(not(sameInstance(stream))));
      Iterator<String> classIterator = targetStream.iterator();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.MAP, TerminalOperation.ITERATOR);
      assertThat(asList(classIterator),
              containsInAnyOrder(ANIMALS.stream().map(a -> nullSafe(a.getTaxonomicClass(), "unknown")).toArray(String[]::new)));
    });
  }

  @Test
  public void testMapPortable() throws Exception {
    tryStream(stream -> {
      Stream<String> targetStream = stream.map(TAXONOMIC_CLASS.valueOr("unknown"));
      assertThat(targetStream, is(not(sameInstance(stream))));
      Iterator<String> classIterator = targetStream.iterator();
      assertPortableOps(stream, IntermediateOperation.MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.ITERATOR);
      assertThat(asList(classIterator),
              containsInAnyOrder(ANIMALS.stream().map(a -> nullSafe(a.getTaxonomicClass(), "unknown")).toArray(String[]::new)));
    });
  }

  @Test
  public void testMapToIntNonPortable() throws Exception {
    tryStream(stream -> {
      IntStream targetStream = stream.mapToInt(r -> r.get(TAXONOMIC_CLASS).orElse("").length());
      assertThat(targetStream, is(not(sameInstance(stream))));
      PrimitiveIterator.OfInt lengthIterator = targetStream.iterator();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.MAP_TO_INT, TerminalOperation.ITERATOR);
      assertThat(asList(lengthIterator),
              containsInAnyOrder(ANIMALS.stream().mapToInt(a -> nullSafe(a.getTaxonomicClass(), "").length()).boxed().toArray()));
    });
  }

  @Test
  public void testMapToLongNonPortable() throws Exception {
    tryStream(stream -> {
      LongStream targetStream = stream.mapToLong(r -> r.get(OBSERVATIONS).orElse(0L));
      assertThat(targetStream, is(not(sameInstance(stream))));
      PrimitiveIterator.OfLong observations = targetStream.iterator();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.MAP_TO_LONG, TerminalOperation.ITERATOR);
      assertThat(asList(observations),
              containsInAnyOrder(ANIMALS.stream().mapToLong(a -> nullSafe(a.getObservations(), 0L)).boxed().toArray()));
    });
  }

  @Test
  public void testMapToDoubleNonPortable() throws Exception {
    tryStream(stream -> {
      DoubleStream targetStream = stream.mapToDouble(r -> r.get(OBSERVATIONS).orElse(0L).doubleValue());
      assertThat(targetStream, is(not(sameInstance(stream))));
      PrimitiveIterator.OfDouble observations = targetStream.iterator();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.MAP_TO_DOUBLE, TerminalOperation.ITERATOR);
      assertThat(asList(observations),
              containsInAnyOrder(ANIMALS.stream().mapToDouble(a -> nullSafe(a.getObservations(), 0L).doubleValue()).boxed().toArray()));
    });
  }

  @Test
  public void testFlatMapNonPortable() throws Exception {
    tryStream(stream -> {
      Stream<Character> targetStream = stream.flatMap(r -> r.getKey().chars().mapToObj(c -> (char)c));
      assertThat(targetStream, is(not(sameInstance(stream))));
      Iterator<Character> chars = targetStream.iterator();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.FLAT_MAP, TerminalOperation.ITERATOR);
      assertThat(asList(chars), containsInAnyOrder(ANIMALS.stream().flatMap(a -> a.getName().chars().mapToObj(c -> (char)c)).toArray()));
    });
  }

  @Test
  public void testFlatMapToIntNonPortable() throws Exception {
    tryStream(stream -> {
      IntStream targetStream = stream.flatMapToInt(r -> r.getKey().chars());
      assertThat(targetStream, is(not(sameInstance(stream))));
      PrimitiveIterator.OfInt chars = targetStream.iterator();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.FLAT_MAP_TO_INT, TerminalOperation.ITERATOR);
      assertThat(asList(chars), containsInAnyOrder(ANIMALS.stream().flatMapToInt(a -> a.getName().chars()).boxed().toArray()));
    });
  }

  @Test
  public void testFlatMapToLongNonPortable() throws Exception {
    tryStream(stream -> {
      LongStream targetStream = stream.flatMapToLong(r -> r.getKey().chars().mapToLong(c -> (long)c));
      assertThat(targetStream, is(not(sameInstance(stream))));
      PrimitiveIterator.OfLong longs = targetStream.iterator();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.FLAT_MAP_TO_LONG, TerminalOperation.ITERATOR);
      assertThat(asList(longs),
              containsInAnyOrder(ANIMALS.stream().flatMapToLong(a -> a.getName().chars().mapToLong(c -> (long)c)).boxed().toArray()));
    });
  }

  @Test
  public void testFlatMapToDoubleNonPortable() throws Exception {
    tryStream(stream -> {
      DoubleStream targetStream = stream.flatMapToDouble(r -> r.getKey().chars().mapToDouble(c -> c * 2.0d));
      assertThat(targetStream, is(not(sameInstance(stream))));
      PrimitiveIterator.OfDouble doubles = targetStream.iterator();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.FLAT_MAP_TO_DOUBLE, TerminalOperation.ITERATOR);
      assertThat(asList(doubles),
              containsInAnyOrder(ANIMALS.stream().flatMapToDouble(r -> r.getName().chars().mapToDouble(c -> c * 2.0d)).boxed().toArray()));
    });
  }

  @Test
  public void testDistinctPortable() throws Exception {
    tryStream(stream -> {
      Iterator<Record<String>> distinctRecords = stream.distinct().iterator();
      assertPortableOps(stream, IntermediateOperation.DISTINCT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.ITERATOR);
      assertContainsInAnyOrder(distinctRecords, ANIMALS.iterator());
    });
  }

  @Test
  public void testSorted0() throws Exception {
    tryStream(stream -> {
      try {
        stream.sorted().count();
        fail();
      } catch (UnsupportedOperationException e) {
        // Since 'sorted()' is unsupported, this is expected to fail before recording any operations
        assertPortableOps(stream);
        assertTerminalPortableOp(stream, null);
        assertNonPortableOps(stream);
      }
    });
  }

  @Test
  public void testSorted1() throws Exception {
    tryStream(stream -> {
      Iterator<Record<String>> sorted = stream.sorted(comparing(TAXONOMIC_CLASS.valueOr(""))).iterator();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.SORTED_1, TerminalOperation.ITERATOR);
      assertThat(() -> sorted, isOrderedAccordingTo(Comparator.<Record<String>, String>comparing(TAXONOMIC_CLASS.valueOr(""))));
    });
  }

  @Test
  public void testPeek() throws Exception {
    tryStream(stream -> {
      Iterator<Record<String>> peekIterator = stream.peek(r -> {}).iterator();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.PEEK, TerminalOperation.ITERATOR);
      assertContainsInAnyOrder(peekIterator, ANIMALS.iterator());
    });
  }

  @Test
  public void testLimit() throws Exception {
    tryStream(stream -> {
      long limitCount = stream.limit(5).count();
      assertPortableOps(stream, IntermediateOperation.LIMIT);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
      assertThat(limitCount, is(5L));
    });
  }

  @Test
  public void testSkip() throws Exception {
    tryStream(stream -> {
      long skipCount = stream.skip(5).count();
      assertPortableOps(stream, IntermediateOperation.SKIP);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
      assertThat(skipCount, is(ANIMALS.size() - 5L));
    });
  }

  @Test
  public void testForEach() throws Exception {
    tryStream(stream -> {
      stream.forEach(r -> {});
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.FOR_EACH);
    });
  }

  @Test
  public void testForEachOrdered() throws Exception {
    tryStream(stream -> {
      stream.forEachOrdered(r -> {});
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.FOR_EACH_ORDERED);
    });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testToArray0() throws Exception {
    tryStream(stream -> {
      Object[] objects = stream.toArray();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.TO_ARRAY_0);
      assertContainsInAnyOrder(Arrays.stream(objects).map(o -> (Record<String>)o).iterator(), ANIMALS.iterator());
    });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testToArray1() throws Exception {
    tryStream(stream -> {
      Record<String>[] records = stream.toArray(Record[]::new);
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.TO_ARRAY_1);
      assertContainsInAnyOrder(Arrays.<Record<String>>asList(records).iterator(), ANIMALS.iterator());
    });
  }

  @Test
  public void testReduce1() throws Exception {
    tryStream(stream -> {
      Optional<Record<String>> mostObserved =
              stream.reduce(BinaryOperator.maxBy(comparing(OBSERVATIONS.valueOr(0L))));
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.REDUCE_1);
      // This assertion is predicated on the fact that one Animal has a "maximal" observation count.
      assertTrue(mostObserved.isPresent());
      assertThat(new Animal(mostObserved.get()),
              is(ANIMALS.stream().reduce(BinaryOperator.maxBy(comparing(a -> (a.getObservations() == null ? 0L : a.getObservations())))).get()));
    });
  }

  @Test
  public void testReduce2() throws Exception {
    tryStream((RootRemoteRecordStream<String> stream) -> {
      Record<String> identity = new IdentityRecord();
      Record<String> mostObserved =
              stream.reduce(identity, BinaryOperator.maxBy(comparing(OBSERVATIONS.valueOr(0L))));
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.REDUCE_2);
      // This assertion is predicated on the fact that one Animal has a "maximal" observation count.
      assertThat(new Animal(mostObserved),
              is(ANIMALS.stream().reduce(new Animal("", null, null),
                      BinaryOperator.maxBy(comparing((Animal a) -> (a.getObservations() == null ? 0L : a.getObservations()))))));
    });
  }

  @Test
  public void testReduce3() throws Exception {
    tryStream(stream -> {
      Long observationTotal = stream.reduce(0L, (sum, r) -> sum + r.get(OBSERVATIONS).orElse(0L), (s1, s2) -> s1 + s2);
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.REDUCE_3);
      assertThat(observationTotal,
              is(ANIMALS.stream().reduce(0L, (sum, a) -> sum + (a.getObservations() == null ? 0L : a.getObservations()), (s1, s2) -> s1 + s2)));
    });
  }

  @Test
  public void testCollect1() throws Exception {
    tryStream(stream -> {
      List<Record<String>> records = stream.collect(toList());
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.COLLECT_1);
      assertContainsInAnyOrder(records.iterator(), ANIMALS.iterator());
    });
  }

  @Test
  public void testCollect3() throws Exception {
    tryStream(stream -> {
      HashMap<String, CellSet> cellMap =
              stream.collect(HashMap<String, CellSet>::new, (map, r) -> map.put(r.getKey(), new CellSet(r)), (m1, m2) -> m2.putAll(m1));
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.COLLECT_3);
      assertThat(cellMap,
              is(equalTo(ANIMALS.stream().collect(HashMap<String, CellSet>::new, (map, a) -> map.put(a.getName(), a.getCells()), (m1, m2) -> m2.putAll(m1)))));
    });
  }

  @Test
  public void testMin() throws Exception {
    tryStream(stream -> {
      Optional<Record<String>> minObservations = stream.min(comparing(OBSERVATIONS.valueOr(0L)));
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.MIN_1);
      assertTrue(minObservations.isPresent());
      Record<String> minObservedRecord = minObservations.get();
      assertThat(new Animal(minObservedRecord),
              isIn(ANIMALS.stream().filter(a -> (a.getObservations() == null ? 0L : a.getObservations()) == minObservedRecord.get(OBSERVATIONS).orElse(0L)).toArray(Animal[]::new)));
    });
  }

  @Test
  public void testMax() throws Exception {
    tryStream(stream -> {
      Optional<Record<String>> maxObservations = stream.max(comparing(OBSERVATIONS.valueOr(0L)));
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.MAX_1);
      assertTrue(maxObservations.isPresent());
      Record<String> maxObservedRecord = maxObservations.get();
      assertThat(new Animal(maxObservedRecord),
              isIn(ANIMALS.stream().filter(a -> (a.getObservations() == null ? 0L : a.getObservations()) == maxObservedRecord.get(OBSERVATIONS).orElse(0L)).toArray(Animal[]::new)));
    });
  }

  @Test
  public void testCount() throws Exception {
    tryStream(stream -> {
      long count = stream.count();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
      assertThat(count, is((long)ANIMALS.size()));
    });
  }

  @Test
  public void testAnyMatch() throws Exception {
    tryStream(stream -> {
      boolean hasArachnid = stream.anyMatch(TAXONOMIC_CLASS.value().is("arachnid"));
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, TerminalOperation.ANY_MATCH);
      assertNonPortableOps(stream);
      assertThat(hasArachnid, is(ANIMALS.stream().anyMatch(a -> a.getTaxonomicClass().equals("arachnid"))));
    });
  }

  @Test
  public void testAllMatch() throws Exception {
    tryStream(stream -> {
      boolean allArachnids = stream.allMatch(TAXONOMIC_CLASS.value().is("arachnid"));
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, TerminalOperation.ALL_MATCH);
      assertNonPortableOps(stream);
      assertThat(allArachnids, is(ANIMALS.stream().allMatch(a -> a.getTaxonomicClass().equals("arachnid"))));
    });
  }

  @Test
  public void testNoneMatch() throws Exception {
    tryStream(stream -> {
      boolean noArachnids = stream.noneMatch(TAXONOMIC_CLASS.value().is("arachnid"));
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, TerminalOperation.NONE_MATCH);
      assertNonPortableOps(stream);
      assertThat(noArachnids, is(ANIMALS.stream().noneMatch(a -> a.getTaxonomicClass().equals("arachnid"))));
    });
  }

  @Test
  public void testFindFirst() throws Exception {
    tryStream(stream -> {
      Optional<Record<String>> first = stream.findFirst();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, TerminalOperation.FIND_FIRST);
      assertNonPortableOps(stream);
      assertTrue(first.isPresent());
    });
  }

  @Test
  public void testFindAny() throws Exception {
    tryStream(stream -> {
      Optional<Record<String>> first = stream.findAny();
      assertPortableOps(stream);
      assertTerminalPortableOp(stream, TerminalOperation.FIND_ANY);
      assertNonPortableOps(stream);
      assertTrue(first.isPresent());
    });
  }

  /**
   * Compares the contents, in any order, of the two {@link Iterator}s provided.  The {@code Record} instances from
   * the {@code actualRecords} {@code Iterator} are converted to {@link Animal} instances for comparison.
   *
   * @param actualRecords an {@code Iterator} over the  actual {@code Record}s
   * @param expectedAnimals an {@code Iterator} over the expected {@code Animal} values
   */
  private void assertContainsInAnyOrder(Iterator<Record<String>> actualRecords, Iterator<Animal> expectedAnimals) {
    Spliterator<Record<String>> actualSpliterator = spliteratorUnknownSize(actualRecords, Spliterator.IMMUTABLE);
    assertContainsInAnyOrder(actualSpliterator, expectedAnimals);
  }

  /**
   * Compares the contents, in any order, of the {@link Spliterator Spliterator<Record<String>>} and the
   * {@link Iterator Iterator<Animal>} provided.  The {@code Record} instances from the {@code actualRecords}
   * {@code Spliterator} are converted to {@link Animal} instances for comparison.
   *
   * @param actualRecords an {@code Spliterator} over the  actual {@code Record}s
   * @param expectedAnimals an {@code Iterator} over the expected {@code Animal} values
   */
  private void assertContainsInAnyOrder(Spliterator<Record<String>> actualRecords, Iterator<Animal> expectedAnimals) {
    List<Animal> actual = stream(actualRecords, false).map(Animal::new).collect(toList());
    Animal[] expected = stream(Spliterators.spliteratorUnknownSize(expectedAnimals, 0), false)
            .toArray(Animal[]::new);
    assertThat(actual, containsInAnyOrder(expected));
  }

  private <V> List<V> asList(Iterator<V> iterator) {
    return stream(spliteratorUnknownSize(iterator, 0), false).collect(toList());
  }

  /**
   * Asserts that the portable operations calculated by pipeline analysis matches the expected sequence.
   * @param stream the {@link RootRemoteRecordStream} from which the portable operations are observed
   * @param operations the expected sequence of non-portable operations
   */
  private void assertPortableOps(RootRemoteRecordStream<String> stream, PipelineOperation.Operation... operations) {
    List<PipelineOperation> portablePipeline = stream.getPortableIntermediatePipelineSequence();
    if (operations.length == 0) {
      assertThat(portablePipeline, is(empty()));
    } else {
      List<PipelineOperation.Operation> actualOperations =
              portablePipeline.stream().map(PipelineOperation::getOperation).collect(toList());
      assertThat(actualOperations, contains(operations));
    }
  }

  /**
   * Asserts that the portable terminal operation calculated by pipeline analysis matches the expected sequence.
   * @param stream the {@link RootRemoteRecordStream} from which the portable operations are observed
   * @param terminalOperation the expected portable terminal operation
   */
  protected void assertTerminalPortableOp(RootRemoteRecordStream<String> stream, PipelineOperation.TerminalOperation terminalOperation) {
    PipelineOperation portableTerminalOperation = stream.getPortableTerminalOperation();
    if (terminalOperation == null) {
      assertThat(portableTerminalOperation, is(nullValue()));
    } else {
      assertThat(portableTerminalOperation.getOperation(), is(terminalOperation));
    }
  }

  /**
   * Asserts that the non-portable operations calculated by pipeline analysis matches the expected sequence.
   * @param stream the {@link RootRemoteRecordStream} from which the non-portable operations are observed
   * @param operations the expected sequence of non-portable operations
   */
  private void assertNonPortableOps(RootRemoteRecordStream<String> stream, PipelineOperation.Operation... operations) {
    WrappedStream<?, ?> rootWrappedStream = stream.getRootWrappedStream();
    List<PipelineOperation> nonPortablePipeline =
            (rootWrappedStream == null ? Collections.emptyList() : rootWrappedStream.getMetaData().getPipeline());
    if (operations.length == 0) {
      assertThat(nonPortablePipeline, is(empty()));
    } else {
      List<PipelineOperation.Operation> actualOperations =
              nonPortablePipeline.stream().map(PipelineOperation::getOperation).collect(toList());
      assertThat(actualOperations, contains(operations));
    }
  }

  /**
   * Runs a test, packaged as a {@link Consumer}, against the {@link RemoteRecordStream} obtained from
   * {@link #getTestStream()}.
   * @param testProc the test procedure
   */
  private void tryStream(Consumer<RootRemoteRecordStream<String>> testProc) throws Exception {
    try (final RootRemoteRecordStream<String> stream = getTestStream()) {
      testProc.accept(stream);
    }
  }

  private RootRemoteRecordStream<String> getTestStream() {
    return animalDataset.getStream();
  }

  private static <V> V nullSafe(V value, V defaultValue) {
    return (value == null ? defaultValue : value);
  }

  private final class IdentityRecord implements Record<String> {
    private final String identityKey = "";
    private final CellCollection emptyCells = new CellSet();

    @Override
    public String getKey() {
      return identityKey;
    }

    @Override
    public <T> Optional<T> get(CellDefinition<T> cellDefinition) {
      return emptyCells.get(cellDefinition);
    }

    @Override
    public Optional<?> get(String name) {
      return emptyCells.get(name);
    }

    @Override
    public int size() {
      return emptyCells.size();
    }

    @Override
    public boolean isEmpty() {
      return emptyCells.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
      return emptyCells.contains(o);
    }

    @Override
    public Iterator<Cell<?>> iterator() {
      return emptyCells.iterator();
    }

    @Override
    public Object[] toArray() {
      return emptyCells.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      return emptyCells.toArray(a);
    }

    @Override
    public boolean add(Cell<?> cell) {
      return emptyCells.add(cell);
    }

    @Override
    public boolean remove(Object o) {
      return emptyCells.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      return emptyCells.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends Cell<?>> c) {
      return emptyCells.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      return emptyCells.removeAll(c);
    }

    @Override
    public boolean removeIf(Predicate<? super Cell<?>> filter) {
      return emptyCells.removeIf(filter);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      return emptyCells.retainAll(c);
    }

    @Override
    public void clear() {
      emptyCells.clear();
    }
  }
}