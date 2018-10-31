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
package com.terracottatech.store.server;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.ExplanationAssertions;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreBusyException;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.client.stream.Explanation;
import com.terracottatech.store.client.stream.RemoteObject;
import com.terracottatech.store.client.stream.sharded.AbstractShardedRecordStream;
import com.terracottatech.store.function.Collectors;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.server.ObservableDatasetEntityServerService.ObservableDatasetActiveEntity;
import com.terracottatech.store.server.stream.InlineElementSource;
import com.terracottatech.store.server.stream.PipelineProcessor;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.store.stream.RecordStream;
import com.terracottatech.test.data.Animals;
import com.terracottatech.tool.Diagnostics;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.connection.Connection;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.exception.ConnectionClosedException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.terracottatech.store.ExplanationAssertions.clientSideMergedPipeline;
import static com.terracottatech.store.ExplanationAssertions.clientSideStripePipeline;
import static com.terracottatech.store.ExplanationAssertions.op;
import static com.terracottatech.store.ExplanationAssertions.serverSidePipeline;
import static com.terracottatech.store.Record.keyFunction;
import static com.terracottatech.store.UpdateOperation.write;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.AS_LONG_STREAM;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.BOXED;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.DELETE_THEN;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.DISTINCT;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.FILTER;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.LIMIT;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MAP;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MAP_TO_DOUBLE;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MAP_TO_INT;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MAP_TO_LONG;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MUTATE_THEN;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MUTATE_THEN_INTERNAL;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.SKIP;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.SORTED_0;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.SORTED_1;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.UNORDERED;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.ALL_MATCH;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.ANY_MATCH;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.COLLECT_1;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.COUNT;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.DELETE;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.FIND_ANY;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.FIND_FIRST;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.FOR_EACH;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.MAX_0;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.MAX_1;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.MIN_0;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.MIN_1;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.MUTATE;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.REDUCE_1;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.SPLITERATOR;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.SUM;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.SUMMARY_STATISTICS;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.TO_ARRAY_0;
import static com.terracottatech.store.function.BuildableFunction.identity;
import static com.terracottatech.store.function.Collectors.counting;
import static com.terracottatech.store.function.Collectors.filtering;
import static com.terracottatech.store.function.Collectors.groupingBy;
import static com.terracottatech.store.function.Collectors.groupingByConcurrent;
import static com.terracottatech.store.function.Collectors.mapping;
import static com.terracottatech.store.function.Collectors.partitioningBy;
import static com.terracottatech.store.function.Collectors.summarizingInt;
import static com.terracottatech.store.function.Collectors.summarizingLong;
import static com.terracottatech.test.data.Animals.ANIMALS;
import static com.terracottatech.test.data.Animals.ENDANGERED;
import static com.terracottatech.test.data.Animals.NOT_ASSESSED;
import static com.terracottatech.test.data.Animals.Schema.IMAGE;
import static com.terracottatech.test.data.Animals.Schema.IS_LISTED;
import static com.terracottatech.test.data.Animals.Schema.MASS;
import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.STATUS;
import static com.terracottatech.test.data.Animals.Schema.STATUS_LEVEL;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static com.terracottatech.tool.WaitForAssert.assertThatEventually;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Tests the multi-stripe remote {@link Stream} functions of {@link DatasetActiveEntity}.
 * TODO: eventually merge this class into {@link RemoteStreamTest}
 */
@SuppressWarnings("Duplicates")
@RunWith(Parameterized.class)
public class MultistripeRemoteStreamTest extends PassthroughTest {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.stream(StreamStrategy.values()).map(v -> new Object[] {v}).collect(toList());
  }

  private final StreamStrategy streamStrategy;

  public MultistripeRemoteStreamTest(StreamStrategy strategy) {
    this.streamStrategy = strategy;
  }

  @Override
  protected List<String> provideStripeNames() {
    return Arrays.asList("stripe1", "stripe2");
  }

  @Rule
  public ExplanationAssertions explanationAssertions = new ExplanationAssertions();

  private Consumer<Object> explanationMatches(Matcher<Explanation> explanationMatcher) {
    return explanationAssertions.explanationMatches(explanationMatcher);
  }

  @Test
  public void testUnconsumedStreamExplanation() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    try (Stream<String> stream = streamStrategy.stream(access)
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
            clientSideStripePipeline(containsOperations(op(MAP))),
            clientSideMergedPipeline(containsOperations(op(SPLITERATOR))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .map(r -> r.get(TAXONOMIC_CLASS).get())) {
      stream.spliterator();
    }
  }

  @SuppressWarnings("try")
  @Test
  public void testUnusedStreamExplanation() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    try (Stream<String> ignored = streamStrategy.stream(access)
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
            clientSideStripePipeline(containsOperations(op(MAP))),
            clientSideMergedPipeline(empty()))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .map(r -> r.get(TAXONOMIC_CLASS).get())) {
    }
  }

  @Test
  public void testNaturallyStripeablePipeline() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    List<String> result = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
            clientSideStripePipeline(containsOperations(op(MAP), op(COLLECT_1))),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .map(r -> r.get(TAXONOMIC_CLASS).get())
        .collect(toList());

    assertThat(result.size(), is((int)Animals.ANIMALS.stream().filter(a -> a.getTaxonomicClass().equals("mammal")).count()));
    assertThat(result, everyItem(is(equalTo("mammal"))));
  }

  @Test
  public void testFullyPortableStreamTerminatedWithCount() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    long result = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(COUNT)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(SUM))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .count();

    assertThat(result, is(Animals.ANIMALS.stream().filter(a -> a.getTaxonomicClass().equals("mammal")).count()));
  }

  @Test
  public void testFullyPortableStreamTerminatedWithAllMatch() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    boolean result = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(ALL_MATCH)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(ALL_MATCH))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .allMatch(TAXONOMIC_CLASS.value().is("mammal"));

    assertThat(result, is(Animals.ANIMALS.stream().filter(a -> a.getTaxonomicClass().equals("mammal")).allMatch(a -> a.getTaxonomicClass().equals("mammal"))));
  }

  @Test
  public void testFullyPortableStreamTerminatedWithAnyMatch() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    boolean result = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(ANY_MATCH)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(ANY_MATCH))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .anyMatch(STATUS.value().is(ENDANGERED));

    assertThat(result, is(Animals.ANIMALS.stream().filter(a -> a.getTaxonomicClass().equals("mammal")).anyMatch(a -> ENDANGERED.equals(a.getStatus()))));
  }

  @Test
  public void testPartiallyPortableStream() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    List<String> result = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP), op(DISTINCT)))),
            clientSideStripePipeline(containsOperations(op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(DISTINCT), op(COLLECT_1))))))
        .map(TAXONOMIC_CLASS.valueOrFail())
        .distinct()
        .collect(java.util.stream.Collectors.toList());

    Collections.sort(result);
    assertThat(result, is(Animals.ANIMALS.stream().map(Animals.Animal::getTaxonomicClass).distinct().sorted().collect(java.util.stream.Collectors.toList())));
  }

  @Test
  public void testLimit() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    List<String> result = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP), op(LIMIT)))),
            clientSideStripePipeline(containsOperations(op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(LIMIT), op(COLLECT_1))))))
        .map(TAXONOMIC_CLASS.valueOrFail())
        .limit(2)
        .collect(java.util.stream.Collectors.toList());

    assertThat(result.size(), is(2));
    assertThat(Animals.ANIMALS.stream().map(Animals.Animal::getTaxonomicClass).collect(java.util.stream.Collectors.toList()).containsAll(result), is(true));
  }

  @Test
  public void testSkip() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    List<String> reference = Animals.ANIMALS.stream().map(Animals.Animal::getTaxonomicClass).collect(java.util.stream.Collectors.toList());

    RecordStream<String> recordStream = streamStrategy.stream(access);
    List<String> result = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP)))),
            clientSideStripePipeline(containsOperations(op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(SKIP), op(COLLECT_1))))))
        .map(TAXONOMIC_CLASS.valueOrFail())
        .skip(reference.size() - 1)
        .collect(java.util.stream.Collectors.toList());

    assertThat(result.size(), is(1));
    assertThat(reference.containsAll(result), is(true));
  }

  @Test
  public void remoteFilterTest() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    List<String> result = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
            clientSideStripePipeline(containsOperations(op(MAP), op(COLLECT_1))),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .map(r -> r.get(TAXONOMIC_CLASS).get())
        .collect(toList());

    assertThat(result.size(), is((int)Animals.ANIMALS.stream().filter(a -> a.getTaxonomicClass().equals("mammal")).count()));
    assertThat(result, everyItem(is(equalTo("mammal"))));
  }

  @Test
  public void remoteFilterAndMapTest() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    List<String> result = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MAP)))),
            clientSideStripePipeline(containsOperations(op(COLLECT_1))),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .map(TAXONOMIC_CLASS.valueOrFail())
        .collect(toList());

    assertThat(result.size(), is((int)Animals.ANIMALS.stream().filter(a -> a.getTaxonomicClass().equals("mammal")).count()));
    assertThat(result, everyItem(is(equalTo("mammal"))));
  }

  @Test
  public void remoteMapToLongTest() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    List<Long> result = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_LONG), op(BOXED)))),
            clientSideStripePipeline(containsOperations(op(COLLECT_1))),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .mapToLong(OBSERVATIONS.longValueOr(-9999L))
        .boxed()
        .collect(toList());

    assertThat(result.size(), is(Animals.ANIMALS.size()));
  }

  @Test
  public void remoteMapToLongOrFailTest() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    try {
      recordStream
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_LONG), op(BOXED)))),
              clientSideStripePipeline(containsOperations(op(COLLECT_1))),
              clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
          .mapToLong(OBSERVATIONS.longValueOrFail())
          .boxed()
          .collect(toList());
      fail("expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  @Test
  public void remoteKeyExtraction() throws StoreException {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    Set<String> result = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP)))),
            clientSideStripePipeline(containsOperations(op(COLLECT_1))),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .map(keyFunction())
        .collect(toSet());

    assertThat(result, is(Animals.ANIMALS.stream().map(Animals.Animal::getName).collect(toSet())));
  }

  @Test
  public void testLongPipeline() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    List<Long> mammalStatus = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MAP_TO_INT), op(AS_LONG_STREAM), op(SORTED_0), op(DISTINCT)))),
            clientSideStripePipeline(containsOperations(op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(DISTINCT), op(BOXED), op(SKIP), op(LIMIT), op(UNORDERED), op(COLLECT_1))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .mapToInt(STATUS_LEVEL.intValueOr(-1))
        .asLongStream()
        .sorted()
        .distinct()
        .boxed()
        .skip(1)
        .limit(3)
        .unordered()
        .collect(toList());

    assertThat(mammalStatus, is(Animals.recordStream()
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .mapToInt(STATUS_LEVEL.intValueOr(-1))
        .asLongStream()
        .sorted()
        .distinct()
        .boxed()
        .skip(1)
        .limit(3)
        .unordered()
        .collect(toList())));
  }

  @Test
  public void testLongishPipeline() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    List<String> mammalStatus = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MAP), op(SORTED_0), op(DISTINCT)))),
            clientSideStripePipeline(containsOperations(op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(DISTINCT), op(SKIP), op(LIMIT), op(UNORDERED), op(COLLECT_1))))))
        .filter(OBSERVATIONS.value().isGreaterThan(0L))
        .map(TAXONOMIC_CLASS.valueOr(NOT_ASSESSED))
        .sorted()
        .distinct()
        .skip(1)
        .limit(3)
        .unordered()
        .collect(toList());

    assertThat(mammalStatus, is(Animals.recordStream()
        .filter(OBSERVATIONS.value().isGreaterThan(0L))
        .map(TAXONOMIC_CLASS.valueOr(NOT_ASSESSED))
        .sorted()
        .distinct()
        .skip(1)
        .limit(3)
        .unordered()
        .collect(toList())));
  }

  @Test
  public void testBinaryPipeline() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(access);
    try {
      List<byte[]> mammalImages = recordStream
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MAP), op(SORTED_0)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(SORTED_0), op(DISTINCT), op(SKIP), op(LIMIT), op(UNORDERED), op(COLLECT_1))))))
          .filter(OBSERVATIONS.value().isGreaterThan(0L))
          .map(IMAGE.valueOr(new byte[0]))
          .sorted()
          .distinct()
          .skip(1)
          .limit(3)
          .unordered()
          .collect(toList());
      fail("Expecting ClassCastException");
    } catch (ClassCastException e) {
      assertThat(e.getMessage(), containsString("[B cannot be cast"));
    }
  }

  @Test
  public void testFullCycle() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    List<Animals.Animal> animals = streamStrategy.stream(writerReader).map(Animals.Animal::new).collect(toList());
    assertThat(animals, containsInAnyOrder(Animals.ANIMALS.toArray()));
  }

  @Test
  public void testCountingCollector() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> records = streamStrategy.stream(writerReader);
    long elementCount = records
            .explain(explanationMatches(allOf(
                    serverSidePipeline(streamStrategy.ignoringPrefix(empty())),
                    clientSideStripePipeline(containsOperations(op(COLLECT_1))),
                    clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
            .collect(Collectors.counting());
    //TODO: different in single-stripe mode
    assertThat(elementCount, is(88L));
  }

  @Test
  public void testGroupingWithReducingCollector() {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    Map<String, Long> classCounts = recordStream
            .explain(explanationMatches(allOf(
                    serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
                    clientSideStripePipeline(containsOperations(op(COLLECT_1))),
                    clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
            .filter(TAXONOMIC_CLASS.value().is("mammal"))
            .collect(groupingBy(TAXONOMIC_CLASS.valueOrFail(), counting()));
    assertThat(classCounts.keySet(), containsInAnyOrder("mammal"));
    assertThat(classCounts.values(), containsInAnyOrder(39L));
  }

  @Test
  public void testGroupingWithIdentityCollector() {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    Map<String, IntSummaryStatistics> statusStats = recordStream
            .explain(explanationMatches(allOf(
                    serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(
                            op(FILTER), op(COLLECT_1)))),
                    clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
            .filter(TAXONOMIC_CLASS.value().is("mammal"))
            .collect(groupingBy(TAXONOMIC_CLASS.valueOrFail(),
                    summarizingInt(STATUS_LEVEL.intValueOr(0))));
    assertThat(statusStats.keySet(), containsInAnyOrder("mammal"));
    IntSummaryStatistics statistics = statusStats.values().iterator().next();
    assertThat(statistics.getCount(), is(39L));
    assertThat(statistics.getSum(), is(44L));
    assertThat(statistics.getMin(), is(0));
    assertThat(statistics.getMax(), is(7));
  }

  @Test
  public void testConcurrentGroupingWithReducingCollector() {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    ConcurrentMap<String, Long> classCounts = recordStream
            .explain(explanationMatches(allOf(
                    serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
                    clientSideStripePipeline(containsOperations(op(COLLECT_1))),
                    clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
            .filter(TAXONOMIC_CLASS.value().is("mammal"))
            .collect(groupingByConcurrent(TAXONOMIC_CLASS.valueOrFail(), counting()));
    assertThat(classCounts.keySet(), containsInAnyOrder("mammal"));
    assertThat(classCounts.values(), containsInAnyOrder(39L));
  }

  @Test
  public void testConcurrentGroupingWithIdentityCollector() {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    Map<String, IntSummaryStatistics> statusStats = recordStream
            .explain(explanationMatches(allOf(
                    serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(
                            op(FILTER), op(COLLECT_1)))),
                    clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
            .filter(TAXONOMIC_CLASS.value().is("mammal"))
            .collect(groupingByConcurrent(TAXONOMIC_CLASS.valueOrFail(),
                    summarizingInt(STATUS_LEVEL.intValueOr(0))));
    assertThat(statusStats.keySet(), containsInAnyOrder("mammal"));
    IntSummaryStatistics statistics = statusStats.values().iterator().next();
    assertThat(statistics.getCount(), is(39L));
    assertThat(statistics.getSum(), is(44L));
    assertThat(statistics.getMin(), is(0));
    assertThat(statistics.getMax(), is(7));
  }

  @Test
  public void testPartitioningWithReducingCollector() {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    Map<Boolean, Long> listedCounts = recordStream
            .explain(explanationMatches(allOf(
                    serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
                    clientSideStripePipeline(containsOperations(op(COLLECT_1))),
                    clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
            .filter(TAXONOMIC_CLASS.value().is("mammal"))
            .collect(partitioningBy(IS_LISTED.isTrue(), counting()));
    assertThat(listedCounts.keySet(), containsInAnyOrder(true, false));
    assertThat(listedCounts.get(true), is(13L));
    assertThat(listedCounts.get(false), is(26L));
  }

  @Test
  public void testPartitioningWithIdentityCollector() {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    Map<Boolean, LongSummaryStatistics> observationStats = recordStream
            .explain(explanationMatches(allOf(
                    serverSidePipeline(streamStrategy.ignoringPrefix(
                            containsOperations(op(FILTER), op(COLLECT_1)))),
                    clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
            .filter(TAXONOMIC_CLASS.value().is("mammal"))
            .collect(partitioningBy(IS_LISTED.isTrue(),
                    summarizingLong(OBSERVATIONS.longValueOr(0L))));
    assertThat(observationStats.keySet(), containsInAnyOrder(true, false));
    assertThat(observationStats.get(true).getCount(), is(13L));
    assertThat(observationStats.get(false).getCount(), is(26L));
  }

  @Test
  public void testMappingCollector() {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    Long mammalCount = recordStream
            .explain(explanationMatches(allOf(
                    serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
                    clientSideStripePipeline(containsOperations(op(COLLECT_1))),
                    clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
            .filter(TAXONOMIC_CLASS.value().is("mammal"))
            .collect(mapping(TAXONOMIC_CLASS.valueOrFail(), counting()));
    assertThat(mammalCount, is(39L));
  }

  @Test
  public void testFilteringCollector() {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    Long mammalCount = recordStream
            .explain(explanationMatches(allOf(
                    serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
                    clientSideStripePipeline(containsOperations(op(COLLECT_1))),
                    clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
            .filter(TAXONOMIC_CLASS.value().is("mammal"))
            .collect(filtering(IS_LISTED.value().is(true),
                    counting()));
    assertThat(mammalCount, is(13L));
  }

  @Test
  public void testSummarizingIntCollector() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> records = streamStrategy.stream(writerReader);
    IntSummaryStatistics stats = records
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(COLLECT_1)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .collect(Collectors.summarizingInt(STATUS_LEVEL.intValueOr(0)));

    assertThat(stats.getCount(), is(88L));
    assertThat(stats.getSum(), is(63L));
    assertThat(stats.getMin(), is(0));
    assertThat(stats.getMax(), is(7));
  }

  @Test
  public void testIntSummaryStatistics() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> records = streamStrategy.stream(writerReader);
    IntSummaryStatistics stats = records
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_INT), op(SUMMARY_STATISTICS)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .mapToInt(STATUS_LEVEL.intValueOr(0)).summaryStatistics();

    assertThat(stats.getCount(), is(88L));
    assertThat(stats.getSum(), is(63L));
    assertThat(stats.getMin(), is(0));
    assertThat(stats.getMax(), is(7));
  }

  @Test
  public void testSummarizingLongCollector() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> records = streamStrategy.stream(writerReader);
    LongSummaryStatistics stats = records
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(COLLECT_1)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .collect(summarizingLong(OBSERVATIONS.longValueOr(0L)));

    assertThat(stats.getCount(), is(88L));
    assertThat(stats.getSum(), is(2901L));
    assertThat(stats.getMin(), is(-1L));
    assertThat(stats.getMax(), is(1000L));
  }

  @Test
  public void testLongSummaryStatistics() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> records = streamStrategy.stream(writerReader);
    LongSummaryStatistics stats = records
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_LONG), op(SUMMARY_STATISTICS)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .mapToLong(OBSERVATIONS.longValueOr(0L)).summaryStatistics();

    assertThat(stats.getCount(), is(88L));
    assertThat(stats.getSum(), is(2901L));
    assertThat(stats.getMin(), is(-1L));
    assertThat(stats.getMax(), is(1000L));
  }

  @Test
  public void testSummarizingDoubleCollector() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> records = streamStrategy.stream(writerReader);
    DoubleSummaryStatistics stats = records
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(COLLECT_1)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .collect(Collectors.summarizingDouble(MASS.doubleValueOr(0.0)));

    assertThat(stats.getCount(), is(88L));
    assertThat(stats.getSum(), is(both(greaterThan(13.18)).and(lessThan(13.19))));
    assertThat(stats.getMin(), is(0.0));
    assertThat(stats.getMax(), is(both(greaterThan(0.98)).and(lessThan(0.99))));
  }

  @Test
  public void testDoubleSummaryStatistics() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> records = streamStrategy.stream(writerReader);
    DoubleSummaryStatistics stats = records
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_DOUBLE), op(SUMMARY_STATISTICS)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .mapToDouble(MASS.doubleValueOr(0.0)).summaryStatistics();

    assertThat(stats.getCount(), is(88L));
    assertThat(stats.getSum(), is(both(greaterThan(13.18)).and(lessThan(13.19))));
    assertThat(stats.getMin(), is(0.0));
    assertThat(stats.getMax(), is(both(greaterThan(0.98)).and(lessThan(0.99))));
  }

  @Test
  public void testTerminalPortability() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      Optional<Record<String>> first = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FIND_ANY)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(FILTER), op(FIND_ANY))))))
          .findFirst();
      assertThat(first.get(), instanceOf(Record.class));
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      long count = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_LONG), op(COUNT)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(SUM))))))
          .mapToLong(OBSERVATIONS.longValueOr(0L)).count();
      assertThat(count, is(88L));
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      OptionalLong firstL = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_LONG), op(SORTED_0), op(FIND_FIRST)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(MIN_1))))))
          .mapToLong(OBSERVATIONS.longValueOr(-99L)).sorted().findFirst();
      assertThat(firstL.getAsLong(), is(-99L));
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      try {
        records
            .explain(explanationMatches(allOf(
                serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MAP_TO_INT), op(FIND_ANY)))),
                clientSideStripePipeline(empty()),
                clientSideMergedPipeline(containsOperations(op(FILTER), op(FIND_ANY))))))
            .filter(STATUS_LEVEL.exists().negate()).mapToInt(STATUS_LEVEL.intValueOrFail()).findFirst();
        fail();
      } catch (NoSuchElementException e) {
        // expected
      }
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      Optional<Optional<Long>> first = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP), op(FIND_ANY)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(FILTER), op(FIND_ANY))))))
          .map(OBSERVATIONS.value()).findFirst();
      assertThat(first.get().get(), is(3L));
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      Optional<Optional<Long>> firstR = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP), op(FIND_ANY)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(FILTER), op(FIND_ANY))))))
          .map(OBSERVATIONS.value()).findAny();
      assertThat(firstR.get(), instanceOf(Optional.class));
      if (firstR.get().isPresent()) {
        assertThat(firstR.get().get(), instanceOf(Long.class));
      }
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      OptionalDouble max = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_DOUBLE), op(MAX_0)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(MAX_1))))))
          .mapToDouble(MASS.doubleValueOr(0.5)).max();
      assertThat(max.getAsDouble(), is(both(greaterThan(0.0)).and(lessThan(1.0))));
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      OptionalDouble min = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_DOUBLE), op(MIN_0)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(MIN_1))))))
          .mapToDouble(MASS.doubleValueOr(0.5)).min();
      assertThat(min.getAsDouble(), is(both(greaterThan(0.0)).and(lessThan(1.0))));
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      OptionalLong max = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_LONG), op(MAX_0)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(MAX_1))))))
          .mapToLong(OBSERVATIONS.longValueOr(0L)).max();
      assertThat(max.getAsLong(), is(1000L));
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      OptionalLong min = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_LONG), op(MIN_0)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(MIN_1))))))
          .mapToLong(OBSERVATIONS.longValueOr(0L)).min();
      assertThat(min.getAsLong(), is(-1L));
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      OptionalInt max = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_INT), op(MAX_0)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(MAX_1))))))
          .mapToInt(STATUS_LEVEL.intValueOr(0)).max();
      assertThat(max.getAsInt(), is(7));
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      OptionalInt min = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_INT), op(MIN_0)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(MIN_1))))))
          .mapToInt(STATUS_LEVEL.intValueOr(1000)).min();
      assertThat(min.getAsInt(), is(2));
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      Optional<Record<String>> max = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAX_1)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(MAX_1))))))
          .max(STATUS_LEVEL.valueOr(0).asComparator());
      assertThat(max.get(), instanceOf(Record.class));
    }
    {
      RecordStream<String> records = streamStrategy.stream(writerReader);
      Optional<Record<String>> min = records
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MIN_1)))),
              clientSideStripePipeline(empty()),
              clientSideMergedPipeline(containsOperations(op(MIN_1))))))
          .min(STATUS_LEVEL.valueOr(0).asComparator());
      assertThat(min.get(), instanceOf(Record.class));
    }
  }

  @Test
  public void testAsComparator() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> taxonomyStream = streamStrategy.stream(writerReader);
    List<String> taxonomies = taxonomyStream
        .explain(System.out::println)
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(SORTED_1)))),
            clientSideStripePipeline(containsOperations(op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(MAP), op(DISTINCT), op(COLLECT_1))))))
        .sorted(TAXONOMIC_CLASS.valueOrFail().asComparator())
        .map(TAXONOMIC_CLASS.valueOrFail())
        .distinct()
        .collect(toList());
    assertThat(taxonomies, contains(Animals.ANIMALS.stream()
        .sorted(comparing(Animals.Animal::getTaxonomicClass))
        .map(Animals.Animal::getTaxonomicClass)
        .distinct()
        .toArray()));

    RecordStream<String> observationStream = streamStrategy.stream(writerReader);
    long[] observations = observationStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(MAP_TO_LONG), op(SORTED_0), op(DISTINCT)))),
            clientSideStripePipeline(containsOperations(op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(DISTINCT), op(TO_ARRAY_0))))))
        .mapToLong(OBSERVATIONS.longValueOr(-999L))
        .sorted()
        .distinct()
        .toArray();
    assertThat(observations, is(Animals.ANIMALS.stream()
        .mapToLong(a -> Optional.ofNullable(a.getObservations()).orElse(-999L))
        .sorted()
        .distinct()
        .toArray()));
  }

  @Test
  public void testAsComparatorReverse() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> taxonomyStream = writerReader.records();
    List<String> taxonomies = taxonomyStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(SORTED_1)))),
            clientSideStripePipeline(containsOperations(op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(MAP), op(DISTINCT), op(COLLECT_1))))))
        .sorted(TAXONOMIC_CLASS.valueOrFail().asComparator().reversed())
        .map(TAXONOMIC_CLASS.valueOrFail())
        .distinct()
        .collect(toList());
    assertThat(taxonomies, contains(Animals.ANIMALS.stream()
        .sorted(comparing(Animals.Animal::getTaxonomicClass).reversed())
        .map(Animals.Animal::getTaxonomicClass)
        .distinct()
        .toArray()));
  }

  @Test
  public void testDoubleStreamPortability() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    double mammalObservations = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MAP_TO_DOUBLE), op(SUM)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(SUM))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .mapToDouble(MASS.doubleValueOr(0.0D))
        .sum();
    assertThat(mammalObservations, is(Animals.ANIMALS.stream()
        .filter(a -> a.getTaxonomicClass().equals("mammal"))
        .mapToDouble(a -> Optional.ofNullable(a.getMass()).orElse(0.0D))
        .sum()));
  }

  @Test
  public void testIntStreamPortability() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    int mammalLevels = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MAP_TO_INT), op(SUM)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(SUM))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .mapToInt(STATUS_LEVEL.intValueOr(0))
        .sum();
    assertThat(mammalLevels, is(Animals.ANIMALS.stream()
        .filter(a -> a.getTaxonomicClass().equals("mammal"))
        .mapToInt(a -> Optional.ofNullable(a.getStatusLevel()).orElse(0))
        .sum()));
  }

  @Test
  public void testLongStreamPortability() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    long mammalObservations = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MAP_TO_LONG), op(SUM)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(SUM))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .mapToLong(OBSERVATIONS.longValueOr(0L))
        .sum();
    assertThat(mammalObservations, is(Animals.ANIMALS.stream()
        .filter(a -> a.getTaxonomicClass().equals("mammal"))
        .mapToLong(a -> Optional.ofNullable(a.getObservations()).orElse(0L))
        .sum()));
  }

  @Ignore("Forcing GC not sufficient -- needs different approach")
  @Test
  public void testPipelineProcessorExecutorExhaustion() throws Exception {
    try {
      DatasetWriterReader<String> writerReader = dataset.writerReader();

      List<RecordStream<String>> openStreams = new ArrayList<>();
      openLoop:
      {
        for (long i = Integer.MAX_VALUE * 2L; i > 0; i--) {
          try {
            RecordStream<String> recordStream = streamStrategy.stream(writerReader);
            Iterator<Record<String>> remoteData = recordStream.iterator();
            remoteData.next();        // Force opening of the pipeline processor
            openStreams.add(recordStream);
          } catch (StoreBusyException e) {
            break openLoop;
          }
        }
        fail("Expecting StoreBusyException");   // Bump iteration count?
      }
      assertThat(openStreams, is(not((empty()))));
    } finally {
      for (int i = 0; i < 10; i++) {
        System.gc();
        System.runFinalization();
        Thread.yield();
      }
    }
  }

  @Test
  public void testCloseBeforeExhausted() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> animalStream = streamStrategy.stream(writerReader);
    Iterator<Record<String>> mammals = animalStream
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .iterator();

    boolean closed = false;
    try {
      while (mammals.hasNext()) {
        Record<String> mammal = mammals.next();
        if (mammal.getKey().equals("grison")) {
          animalStream.close();
          closed = true;
        }
      }
      fail("Expecting exception indicating closure");
    } catch (IllegalStateException e) {
      if (!closed) {
        throw new AssertionError("Unexpected exception", e);
      }
      assertThat(e.getMessage(), containsString("closed"));
    }
  }

  @Test
  public void testExceptionInStreamClosesSpliterator() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> animalStream = streamStrategy.stream(writerReader);
    try {
      final AtomicInteger counter = new AtomicInteger();
      animalStream.filter(TAXONOMIC_CLASS.value().is("mammal"))
              .peek(e -> {
                if (counter.getAndIncrement() > provideStripeNames().size()) {
                  throw new IllegalStateException("boom!");
                }
              }).collect(toList());
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      if (streamStrategy.equals(StreamStrategy.PARALLEL)) {
        try {
          assertThat(e.getMessage(), is("boom!"));
        } catch (AssertionError ae1) {
          /*
           * If the failure trips in a FJ thread then the exception gets rewrapped to show it's FJ-nature.  So we must
           * assert on the cause of the exception not the one we catch.
           */
          try {
            e.printStackTrace();
            assertThat(e.getCause().getMessage(), is("boom!"));
          } catch (AssertionError ae2) {
            ae2.addSuppressed(ae1);
            throw ae2;
          }
        }
      } else {
        assertThat(e.getMessage(), is("boom!"));
      }
    }

    List<ObservableDatasetActiveEntity<String>> entities = getObservableDatasetActiveEntity();
    if (streamStrategy.equals(StreamStrategy.PARALLEL)) {
      try {
        assertThatEventually(() -> entities.stream().flatMap(e -> getPipelineProcessors(e).stream()).collect(toList()), empty()).within(Duration.ofSeconds(60));
      } catch (AssertionError e) {
        Diagnostics.threadDump();
        throw e;
      }
    } else {
      assertThat(entities.stream().flatMap(e -> getPipelineProcessors(e).stream()).collect(toList()), empty());
    }
  }

  @Test
  public void testCloseBeforeStarted() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> animalStream = streamStrategy.stream(writerReader);
    animalStream.close();
    try {
      @SuppressWarnings("unused") RecordStream<String> mammals = animalStream
          .filter(TAXONOMIC_CLASS.value().is("mammal"));
      fail("Expecting IllegalStateException for closed stream");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("closed"));
    }
  }

  @Test
  public void testCloseDuringMutation() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> animalStream = streamStrategy.stream(writerReader);
    try {
      animalStream
          .mutate(UpdateOperation.custom(r -> {
            animalStream.close();
            return Collections.emptyList();
          }));
      fail("Expecting IllegalStateException for closed stream");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("closed"));
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void testCloseBeforeRelease() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> animalStream = streamStrategy.stream(writerReader);
    Spliterator<Record<String>> spliterator = animalStream
        .peek(r -> animalStream.close())
        .spliterator();
    try {
          /*
           * Completion of the first iteration sends a Release but does not check it's response until
           * the fetch for the second element is done.  (Because of the close check at the head of the
           * AbstractInlineRemoteSpliterator.consume method, the response from the Release message is never
           * checked.
           */
      for (int i = 0; i < 2; i++) {
        spliterator
            .tryAdvance(r -> { });
      }
      fail("Expecting IllegalStateException for closed stream");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("closed"));
    }
  }

  @Test
  public void testCloseWhileStillForked() throws Exception {
    assumeThat(streamStrategy, is(StreamStrategy.PARALLEL));

    /*
     * We use our own ForkJoinPool so that we can ensure the threads are returned and not stuck but asserting on the
     * quiescent nature of the pool.  Tragedy of the commons prevents this when using the common pool.  JDK convention
     * is that parallel stream tasks spawned from within a FJ-Task use the same pool as their spawner - we also assert
     * on this to make sure it holds true.
     */
    ForkJoinPool fjp = new ForkJoinPool(2);
    try {
      DatasetWriterReader<String> writerReader = dataset.writerReader();

      try (RecordStream<String> stream = streamStrategy.stream(writerReader)) {
        ForkJoinTask<?> result = fjp.submit(() -> {
          Thread initialThread = Thread.currentThread();

          return stream.peek(r -> {
            Thread thread = Thread.currentThread();

            if (thread.equals(initialThread)) {
              throw new IllegalStateException("boom!");
            } else {
              assertThat(thread, instanceOf(ForkJoinWorkerThread.class));
              assertThat(((ForkJoinWorkerThread) thread).getPool(), sameInstance(fjp));

              try {
                Thread.sleep(50);
              } catch (Throwable t) {
                throw new AssertionError(t);
              }
            }
          }).count();
        });

        try {
          result.get();
          fail("Expected ExecutionException");
        } catch (ExecutionException e) {
          assertThat(e.getCause(), instanceOf(IllegalStateException.class));
        }
      }

      /*
       * Failure of the "main" stream causes early termination of the whole stream without waiting for the FJ threads to
       * arrive.  Let's assert that they get released and returned (with associated remote cleanup) in a timely manner.
       */

      List<ObservableDatasetActiveEntity<String>> entities = getObservableDatasetActiveEntity();
      assertThatEventually(() -> entities.stream().flatMap(e -> getPipelineProcessors(e).stream()).collect(toList()), empty()).within(Duration.ofSeconds(10));
      assertThatEventually(fjp::isQuiescent, is(true)).within(Duration.ofSeconds(30));
    } finally {
      fjp.shutdown();
    }
  }

  @Test
  public void testCloseFromExceptionInForkedThread() throws Exception {
    assumeThat(streamStrategy, is(StreamStrategy.PARALLEL));

    /*
     * We use our own ForkJoinPool so that we can ensure the threads are returned and not stuck but asserting on the
     * quiescent nature of the pool.  Tragedy of the commons prevents this when using the common pool.  JDK convention
     * is that parallel stream tasks spawned from within a FJ-Task use the same pool as their spawner - we also assert
     * on this to make sure it holds true.
     */
    ForkJoinPool fjp = new ForkJoinPool(2);
    try {
      DatasetWriterReader<String> writerReader = dataset.writerReader();

      try (RecordStream<String> stream = streamStrategy.stream(writerReader)) {
        ForkJoinTask<?> result = fjp.submit(() -> {
          Thread initialThread = Thread.currentThread();

          return stream.peek(r -> {
            Thread thread = Thread.currentThread();

            if (thread.equals(initialThread)) {
              try {
                Thread.sleep(50);
              } catch (Throwable t) {
                throw new AssertionError(t);
              }
            } else {
              assertThat(thread, instanceOf(ForkJoinWorkerThread.class));
              assertThat(((ForkJoinWorkerThread) thread).getPool(), sameInstance(fjp));

              try {
                Thread.sleep(200);
              } catch (InterruptedException e) {
                throw new AssertionError(e);
              }

              throw new IllegalStateException("boom!");
            }
          }).count();
        });

        try {
          result.get();
          fail("Expected ExecutionException");
        } catch (ExecutionException e) {
          StringWriter writer = new StringWriter();
          e.getCause().printStackTrace(new PrintWriter(writer));
          assertThat("Expected IllegalStateException. Got:\n" + writer.toString(), e.getCause(), instanceOf(IllegalStateException.class));
        }
      }

      /*
       * Failure of the "main" stream causes early termination of the whole stream without waiting for the FJ threads to
       * arrive.  Let's assert that they get released and returned (with associated remote cleanup) in a timely manner.
       */

      List<ObservableDatasetActiveEntity<String>> entities = getObservableDatasetActiveEntity();
      assertThatEventually(() -> entities.stream().flatMap(e -> getPipelineProcessors(e).stream()).collect(toList()), empty()).within(Duration.ofSeconds(10));
      assertThatEventually(fjp::isQuiescent, is(true)).within(Duration.ofSeconds(30));
    } finally {
      fjp.shutdown();
    }
  }

  @Test
  public void testOnClose() throws Exception {
    AtomicBoolean closeHandlerCalled = new AtomicBoolean(false);
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    Iterator<Animals.Animal> animalIterator = streamStrategy.stream(writerReader)
        .onClose(() -> closeHandlerCalled.set(true))
        .map(Animals.Animal::new)
        .iterator();
    while (animalIterator.hasNext()) {
      Animals.Animal animal = animalIterator.next();
      if (animal.getName().equals("kinkajou")) {
        // Partial navigation without explicit Stream closure
        break;
      }
    }
    dataset.close();
    assertTrue(closeHandlerCalled.get());
  }

  @Test
  public void testPortableLimitSkip() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    List<Animals.Animal> someMammals = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
            clientSideStripePipeline(containsOperations(op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(SKIP), op(LIMIT), op(MAP), op(COLLECT_1))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .skip(5)
        .limit(5)
        .map(Animals.Animal::new)
        .collect(toList());

    assertThat(someMammals.size(), is(5));
    assertThat(someMammals, everyItem(isIn(Animals.ANIMALS)));
  }

  @Test
  public void testNonPortableSorted() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    List<Animals.Animal> someMammals = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
            clientSideStripePipeline(containsOperations(op(SORTED_1), op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(MAP), op(COLLECT_1))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .sorted(comparingLong(OBSERVATIONS.longValueOr(0L))
            .thenComparing(Record::getKey))
        .map(Animals.Animal::new)
        .collect(toList());

    assertThat(someMammals,
        is(ANIMALS.stream()
            .filter(a -> a.getTaxonomicClass().equals("mammal"))
            .sorted(comparingLong((Animals.Animal a) -> Optional.ofNullable(a.getObservations()).orElse(0L))
                .thenComparing(Animals.Animal::getName))
            .collect(toList())));
  }

  @Test
  public void testPortableSorted() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> recordStream = streamStrategy.stream(writerReader);
    List<Animals.Animal> someMammals = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(SORTED_1)))),
            clientSideStripePipeline(containsOperations(op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(MAP), op(COLLECT_1))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .sorted(Record.<String>keyFunction().asComparator())
        .map(Animals.Animal::new)
        .collect(toList());

    assertThat(someMammals,
        is(ANIMALS.stream()
            .filter(a -> a.getTaxonomicClass().equals("mammal"))
            .sorted(comparing(Animals.Animal::getName))
            .collect(toList())));
  }

  @Test
  public void testFullyPortableIntermediateMutation() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
    long mutatedCount = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MUTATE_THEN), op(COUNT)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(SUM))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .mutateThen(write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment()))
        .count();

    Animals.recordStream()
        .forEach(
            animalRecord -> {
              String key = animalRecord.getKey();
              Record<String> datasetRecord = writerReader.get(key)
                  .orElseThrow(() -> new AssertionError("Record['" + key + "'] missing"));
              if (animalRecord.get(TAXONOMIC_CLASS).map("mammal"::equals).orElse(false)) {
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(1 + animalRecord.get(OBSERVATIONS).orElse(0L)));
              } else {
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(animalRecord.get(OBSERVATIONS).orElse(0L)));
              }
            }
        );

    assertThat(mutatedCount, is(Animals.recordStream().filter(TAXONOMIC_CLASS.value().is("mammal")).count()));
  }

  @Test
  public void testPartiallyPortableIntermediateMutation() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
    Map<String, Record<String>> resultMap = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MUTATE_THEN)))),
            clientSideStripePipeline(containsOperations(op(MAP), op(COLLECT_1))),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .mutateThen(write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment()))
        .map(Tuple::getSecond)
        .collect(toMap(Record::getKey, identity()));

    Animals.recordStream()
        .forEach(
            animalRecord -> {
              String key = animalRecord.getKey();
              Record<String> datasetRecord = writerReader.get(key)
                  .orElseThrow(() -> new AssertionError("Record['" + key + "'] missing"));
              if (animalRecord.get(TAXONOMIC_CLASS).map("mammal"::equals).orElse(false)) {
                assertThat(resultMap.keySet(), hasItem(key));
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(1 + animalRecord.get(OBSERVATIONS).orElse(0L)));
              } else {
                assertThat(resultMap.keySet(), not(hasItem(key)));
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(animalRecord.get(OBSERVATIONS).orElse(0L)));
              }
            }
        );
  }

  @Test
  public void testNonPortableIntermediateMutationPortableTransform() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
    Map<String, Record<String>> resultMap = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(FILTER), op(MUTATE_THEN_INTERNAL)))),
            clientSideStripePipeline(containsOperations(op(FILTER), op(MAP), op(MAP), op(COLLECT_1))),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .filter(r -> !r.getKey().equals("echidna"))               // Non-portable
        .mutateThen(write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment()))
        .map(Tuple.second())
        .collect(toMap(Record::getKey, identity()));

    Animals.recordStream()
        .forEach(
            animalRecord -> {
              String key = animalRecord.getKey();
              Record<String> datasetRecord = writerReader.get(key)
                  .orElseThrow(() -> new AssertionError("Record['" + key + "'] missing"));
              if (!key.equals("echidna") && animalRecord.get(TAXONOMIC_CLASS).map("mammal"::equals).orElse(false)) {
                assertThat(resultMap.keySet(), hasItem(key));
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(1 + animalRecord.get(OBSERVATIONS).orElse(0L)));
              } else {
                assertThat(resultMap.keySet(), not(hasItem(key)));
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(animalRecord.get(OBSERVATIONS).orElse(0L)));
              }
            }
        );
  }

  @Test
  public void testPartiallyPortableIntermediateMutationPortableTransform() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
    Map<String, Record<String>> resultMap = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(FILTER), op(MUTATE_THEN), op(MAP)))),
            clientSideStripePipeline(containsOperations(op(COLLECT_1))),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .filter(Record.<String>keyFunction().is("echidna").negate())
        .mutateThen(write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment()))
        .map(Tuple.second())
        .collect(toMap(Record::getKey, identity()));

    Animals.recordStream()
        .forEach(
            animalRecord -> {
              String key = animalRecord.getKey();
              Record<String> datasetRecord = writerReader.get(key)
                  .orElseThrow(() -> new AssertionError("Record['" + key + "'] missing"));
              if (!key.equals("echidna") && animalRecord.get(TAXONOMIC_CLASS).map("mammal"::equals).orElse(false)) {
                assertThat(resultMap.keySet(), hasItem(key));
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(1 + animalRecord.get(OBSERVATIONS).orElse(0L)));
              } else {
                assertThat(resultMap.keySet(), not(hasItem(key)));
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(animalRecord.get(OBSERVATIONS).orElse(0L)));
              }
            }
        );
  }

  @Test
  public void testFullyPortableTerminalMutation() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
    recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MUTATE_THEN), op(COUNT)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(empty()))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .mutate(write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment()));

    Animals.recordStream()
        .forEach(
            animalRecord -> {
              String key = animalRecord.getKey();
              Record<String> datasetRecord = writerReader.get(key)
                  .orElseThrow(() -> new AssertionError("Record['" + key + "'] missing"));
              if (animalRecord.get(TAXONOMIC_CLASS).map("mammal"::equals).orElse(false)) {
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(1 + animalRecord.get(OBSERVATIONS).orElse(0L)));
              } else {
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(animalRecord.get(OBSERVATIONS).orElse(0L)));
              }
            }
        );
  }

  @Test
  public void testNonPortableTerminalMutationPortableTransform() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
    recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(FILTER), op(MUTATE)))),
            clientSideStripePipeline(containsOperations(op(FILTER), op(FOR_EACH))),
            clientSideMergedPipeline(empty()))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .filter(r -> !r.getKey().equals("echidna"))               // Non-portable
        .mutate(write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment()));

    Animals.recordStream()
        .forEach(
            animalRecord -> {
              String key = animalRecord.getKey();
              Record<String> datasetRecord = writerReader.get(key)
                  .orElseThrow(() -> new AssertionError("Record['" + key + "'] missing"));
              if (!key.equals("echidna") && animalRecord.get(TAXONOMIC_CLASS).orElse("").equals("mammal")) {
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(1 + animalRecord.get(OBSERVATIONS).orElse(0L)));
              } else {
                assertThat(datasetRecord.get(OBSERVATIONS).orElse(0L), is(animalRecord.get(OBSERVATIONS).orElse(0L)));
              }
            }
        );
  }

  /**
   * Ensure {@code sorted} for a mutative pipeline is rejected.
   * <p>
   * {@code MutableRecordStream.sorted} returns {@link RecordStream} and can thus not precede a mutation
   * operation.
   * <p>
   * If {@code MutableRecordStream} with a {@code sorted} operation <i>before</i> a mutative operation were
   * permitted, success or failure of the operation would depend on whether the pipeline segment up and including
   * the mutative operation were portable.  With a non-portable stream like the following:
   * <pre>{@code
   * stream
   *   .filter( <<nonPortablePredicate>> )
   *   .sorted( <<portableComparator>> )
   *   .mutateThen( <<portableTransform>> )
   *   .map(Tuple::second)
   *   .collect(toList())
   * }</pre>
   * the {@code sorted} operation completely drains the remote {@code Spliterator}.  When the first
   * element exits the sort and reaches the client-side {@code mutateThen} "operation", the mutate
   * request to the server would find no elements in the pipeline if it were not for the fact that
   * the spliterator exhaustion resulted in closing the remote stream.
   */
  @Test
  public void testPreMutationSorted() throws Exception {
    Method sortedMethod = MutableRecordStream.class.getMethod("sorted", Comparator.class);
    assertThat(sortedMethod.getReturnType(),
        allOf(Matchers.<Class<?>>is(RecordStream.class), is(not(MutableRecordStream.class))));
    sortedMethod = MutableRecordStream.class.getMethod("sorted");
    assertThat(sortedMethod.getReturnType(),
        allOf(Matchers.<Class<?>>is(RecordStream.class), is(not(MutableRecordStream.class))));
  }

  @Test
  public void testPostMutationSorted() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    Predicate<Record<?>> unseen = OBSERVATIONS.longValueOr(0L).is(0L);
    assertThat(unseen, is(instanceOf(Intrinsic.class)));
    MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
    List<Record<String>> newlyEndangered = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(MUTATE_THEN)))),
            clientSideStripePipeline(containsOperations(op(MAP), op(SORTED_1), op(SPLITERATOR))),
            clientSideMergedPipeline(containsOperations(op(COLLECT_1))))))
        .filter(unseen)
        .mutateThen(write(OBSERVATIONS).value(1L))
        .map(Tuple::getSecond)
        .sorted(TAXONOMIC_CLASS.valueOr("").asComparator())
        .collect(toList());

    assertThat(newlyEndangered, not(hasSize(0)));

    Animals.recordStream()
        .forEach(
            animalRecord -> {
              String key = animalRecord.getKey();
              Record<String> datasetRecord = writerReader.get(key)
                  .orElseThrow(() -> new AssertionError("Record['" + key + "'] missing"));
              if (unseen.test(animalRecord)) {
                assertThat(datasetRecord, isIn(newlyEndangered));
              } else {
                assertThat(new Animals.AnimalRecord(datasetRecord), is(animalRecord));
              }
            }
        );
  }

  /**
   * Ensures a {@code sorted} operation is accepted against a {@link MutableRecordStream} and
   * converts that stream to a non-mutable {@link RecordStream}.
   */
  @Test
  public void testMutableStreamSort() throws Exception {
    Comparator<Record<String>> observationOrder = Record.<String>keyFunction().asComparator();
    assertThat(observationOrder, is(instanceOf(Intrinsic.class)));
    Predicate<Record<?>> isMammal = TAXONOMIC_CLASS.value().is("mammal");
    assertThat(isMammal, is(instanceOf(Intrinsic.class)));

    List<Record<String>> expectedOrderedMammals = Animals.recordStream()
        .filter(isMammal)
        .sorted(observationOrder)
        .limit(10)
        .collect(toList());

    DatasetWriterReader<String> writerReader = dataset.writerReader();

        /*
         * Check portable sorted.
         */
    {
      MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
      RecordStream<String> sortedRecordStream = recordStream
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(SORTED_1), op(LIMIT)))),
              clientSideStripePipeline(containsOperations(op(SPLITERATOR))),
              clientSideMergedPipeline(containsOperations(op(LIMIT), op(MAP), op(COLLECT_1))))))
          .filter(isMammal)
          .sorted(observationOrder);
      assertThat(sortedRecordStream,
          is(allOf(instanceOf(RecordStream.class), not(instanceOf(MutableRecordStream.class)))));
      List<Record<String>> orderedMammals = sortedRecordStream
          .limit(10)
          .map(Animals.AnimalRecord::new)
          .collect(toList());

      assertThat(orderedMammals, is(expectedOrderedMammals));
    }

        /*
         * Work with a non-portable sorted
         */
    {
      MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
      RecordStream<String> sortedRecordStream = recordStream
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER)))),
              clientSideStripePipeline(containsOperations(op(SORTED_1), op(LIMIT), op(SPLITERATOR))),
              clientSideMergedPipeline(containsOperations(op(LIMIT), op(MAP), op(COLLECT_1))))))
          .filter(isMammal)
          .sorted(observationOrder::compare);  // non-portable
      assertThat(sortedRecordStream,
          is(allOf(instanceOf(RecordStream.class), not(instanceOf(MutableRecordStream.class)))));
      List<Record<String>> orderedMammals = sortedRecordStream
          .limit(10)
          .map(Animals.AnimalRecord::new)
          .collect(toList());

      assertThat(orderedMammals, is(expectedOrderedMammals));
    }

        /*
         * Work with sorted in a non-portable pipeline.
         */
    {
      MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
      RecordStream<String> sortedRecordStream = recordStream
          .explain(explanationMatches(allOf(
              serverSidePipeline(streamStrategy.ignoringPrefix(empty())),
              clientSideStripePipeline(containsOperations(op(FILTER), op(SORTED_1), op(LIMIT), op(SPLITERATOR))),
              clientSideMergedPipeline(containsOperations(op(LIMIT), op(MAP), op(COLLECT_1))))))
          .filter(isMammal::test)   // non-portable
          .sorted(observationOrder);
      assertThat(sortedRecordStream,
          is(allOf(instanceOf(RecordStream.class), not(instanceOf(MutableRecordStream.class)))));
      List<Record<String>> orderedMammals = sortedRecordStream
          .limit(10)
          .map(Animals.AnimalRecord::new)
          .collect(toList());

      assertThat(orderedMammals, is(expectedOrderedMammals));
    }
  }

  @Test
  public void testFullyPortableIntermediateDeletion() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
    long deletionCount = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(DELETE_THEN), op(COUNT)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(containsOperations(op(SUM))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .deleteThen()
        .count();

    Animals.recordStream()
        .forEach(
            animalRecord -> {
              String key = animalRecord.getKey();
              Optional<Record<String>> recordOptional = writerReader.get(key);
              if (animalRecord.get(TAXONOMIC_CLASS).map("mammal"::equals).orElse(false)) {
                recordOptional.ifPresent(r -> new AssertionError("Record['" + key + "'] present"));
              } else {
                recordOptional.orElseThrow(() -> new AssertionError("Record['" + key + "'] not present"));
              }
            }
        );

    assertThat(deletionCount, is(Animals.recordStream().filter(TAXONOMIC_CLASS.value().is("mammal")).count()));
  }

  @Test
  public void testNonPortableIntermediateDeletion() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
    Map<String, Record<String>> resultMap = recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(FILTER), op(DELETE_THEN)))),
            clientSideStripePipeline(containsOperations(op(FILTER), op(MAP), op(COLLECT_1))),
            clientSideMergedPipeline(containsOperations(op(REDUCE_1))))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .filter(r -> !r.getKey().equals("echidna"))               // Non-portable
        .deleteThen()
        .collect(toMap(Record::getKey, identity()));

    Animals.recordStream()
        .forEach(
            animalRecord -> {
              String key = animalRecord.getKey();
              Optional<Record<String>> recordOptional = writerReader.get(key);
              if (!key.equals("echidna") && animalRecord.get(TAXONOMIC_CLASS).map("mammal"::equals).orElse(false)) {
                assertThat(resultMap.keySet(), hasItem(key));
                recordOptional.ifPresent(r -> new AssertionError("Record['" + key + "'] present"));
              } else {
                assertThat(resultMap.keySet(), not(hasItem(key)));
                recordOptional.orElseThrow(() -> new AssertionError("Record['" + key + "'] not present"));
              }
            }
        );
  }

  @Test
  public void testNonPortableTerminalDeletion() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
    recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(FILTER), op(DELETE)))),
            clientSideStripePipeline(containsOperations(op(FILTER), op(FOR_EACH))),
            clientSideMergedPipeline(empty()))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .filter(r -> !r.getKey().equals("echidna"))             // Non-portable
        .delete();

    Animals.recordStream()
        .forEach(
            animalRecord -> {
              String key = animalRecord.getKey();
              Optional<Record<String>> recordOptional = writerReader.get(key);
              if (!key.equals("echidna") && animalRecord.get(TAXONOMIC_CLASS).map("mammal"::equals).orElse(false)) {
                recordOptional.ifPresent(r -> new AssertionError("Record['" + key + "'] present"));
              } else {
                recordOptional.orElseThrow(() -> new AssertionError("Record['" + key + "'] not present"));
              }
            }
        );
  }

  @Test
  public void testFullyPortableTerminalDeletion() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    MutableRecordStream<String> recordStream = streamStrategy.stream(writerReader);
    recordStream
        .explain(explanationMatches(allOf(
            serverSidePipeline(streamStrategy.ignoringPrefix(containsOperations(op(FILTER), op(DELETE_THEN), op(COUNT)))),
            clientSideStripePipeline(empty()),
            clientSideMergedPipeline(empty()))))
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .delete();

    Animals.recordStream()
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .forEach(
            animalRecord -> {
              String key = animalRecord.getKey();
              Optional<Record<String>> recordOptional = writerReader.get(key);
              if (animalRecord.get(TAXONOMIC_CLASS).map("mammal"::equals).orElse(false)) {
                recordOptional.ifPresent(r -> new AssertionError("Record['" + key + "'] present"));
              } else {
                recordOptional.orElseThrow(() -> new AssertionError("Record['" + key + "'] not present"));
              }
            }
        );
  }

  @Test
  public void testClientDisconnectClosesInlineElementSourceThread() throws Exception {
    assumeThat(streamStrategy, is(StreamStrategy.INLINE));

    Connection connection = getConnection();
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> animalStream = streamStrategy.stream(writerReader);
    try {
      Iterator<Record<String>> mammals = animalStream
          .filter(TAXONOMIC_CLASS.value().is("mammal"))
          .iterator();
      while (mammals.hasNext()) {
        Record<String> mammal = mammals.next();
        if (mammal.getKey().equals("hedgehog")) {

          assertTrue(Arrays.stream(Diagnostics.getAllThreads())
              .map(Thread::getName)
              .anyMatch(name -> name.startsWith("PipelineProcessor")));

          /*
           * Force connection closure on the client side.  This actually causes the client to
           * send a RELEASE_ENTITY message which results in calling DatasetActiveEntity.disconnected.
           */
          connection.close();
          break;
        }
      }

      threadGone:
      {
        for (int i = 10; i > 0; i--) {
          boolean found = Arrays.stream(Diagnostics.getAllThreads())
              .map(Thread::getName)
              .anyMatch(name -> name.startsWith("PipelineProcessor"));
          if (!found) {
            break threadGone;
          }
          Thread.sleep(200L);
        }
        fail("PipelineProcessor thread not disposed as expected");
      }
    } finally {
      try {
        animalStream.close();
      } catch (ConnectionClosedException e) {
        //expected
      }
    }

    dataset = null;   // Prevent closure
  }

  @Test
  public void testRemoteClose() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    for (int i = 256; i > 0; i--) {
      RecordStream<String> animalStream = streamStrategy.stream(writerReader);
      Iterator<Record<String>> mammals = animalStream
          .filter(TAXONOMIC_CLASS.value().is("mammal"))
          .iterator();
      while (mammals.hasNext()) {
        Record<String> mammal = mammals.next();
        if (mammal.getKey().equals("hedgehog")) {
          animalStream.close();
          break;
        }
      }
    }
  }

  @Test
  public void testEmptyStream() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    // first delete everything
    streamStrategy.stream(writerReader).delete();
    // then get the empty stream
    Optional<Record<String>> first = streamStrategy.stream(writerReader).findFirst();
    assertThat(first, is(Optional.empty()));
  }

  /**
   * Tests for failure identified in TDB-1570.  The underlying problem was a race condition involving
   * closure of an "old" and exhausted stream where the element producer thread is returned to the pool
   * and reused before the close message for that pipeline processor source is handled.  It was possible for a
   * close against a "finished" {@link PipelineProcessor PipelineProcessor}
   * to cause the interruption of a new {@code PipelineProcessor}.
   */
  @Test
  public void testConcurrentStreams() throws Exception {
    AtomicReference<Exception> t1Fault = new AtomicReference<>();
    AtomicReference<Exception> t2Fault = new AtomicReference<>();
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    Thread t1 = new Thread(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          streamStrategy.stream(writerReader).findFirst().get();
        }
      } catch (Exception e) {
        t1Fault.set(e);
      }
    });
    t1.start();

    Thread t2 = new Thread(() -> {
      try {
        for (int i=0;i<15;i++) {
          Iterator<Record<String>> it = streamStrategy.stream(writerReader).iterator();
          while (it.hasNext()) {
            Record<String> next = it.next();
            next.getClass();
          }
        }
      } catch (Exception e) {
        t2Fault.set(e);
      }
    });
    t2.start();

    t2.join();
    t1.interrupt();
    t1.join();

    Exception t1Exception = t1Fault.get();
    Exception t2Exception = t2Fault.get();
    if (t1Exception != null || t2Exception != null) {
      AssertionError failure = new AssertionError("Iterating thread(s) failed");
      if (t1Exception != null) {
        failure.addSuppressed(t1Exception);
      }
      if (t2Exception != null) {
        failure.addSuppressed(t2Exception);
      }
      throw failure;
    }
  }

  @Test
  public void testTDB_1808_StreamClosingWithoutMap() throws Exception {
    streamStrategy.stream(dataset.reader())
        .iterator()
        .forEachRemaining(r -> { });
  }

  @Test
  public void testTDB_1808_StreamClosingWithMap() throws Exception {
    streamStrategy.stream(dataset.reader())
        .map(Record::getKey)
        .iterator()
        .forEachRemaining(k -> { });
  }

  @Test
  public void testClosureOfStreamCrossesMerge() throws Exception {
    org.apache.log4j.Logger logger = LogManager.getLogger("com.terracottatech.store.common.dataset.stream.AbstractWrappedStream");
    Appender appender = spy(
        new ConsoleAppender(new PatternLayout("%nLoggingEvent: level=%p, loggerName=%c{1}, message=\"%m\"%n-> from %l%n")));
    logger.addAppender(appender);
    try {
      try (RecordStream<String> stream = streamStrategy.stream(dataset.reader())) {
        stream.iterator();
      }

      for (int i = 0; i < 10; i++) {
        System.gc();
        System.runFinalization();
        Thread.yield();
      }

      verify(appender, never()).doAppend(any(LoggingEvent.class));
    } finally {
      logger.removeAppender(appender);
    }
  }

  /**
   * This test attempts to ensure that a remote {@code InlineElementSource} does not <i>pre-fetch</i> elements
   * from the source stream.  While not an issue for non-mutative pipelines, a pre-fetch in the presence
   * of mutative operations could result in an undesired mutation when the pipeline includes a
   * short-circuiting operation or is closed before exhaustion by the client.
   */
  @Test
  public void testInlineRemoteStreamPreFetch() throws Exception {
    assumeThat(streamStrategy, is(StreamStrategy.INLINE));

    List<ObservableDatasetActiveEntity<String>> observableActiveEntities = getObservableDatasetActiveEntity();

    DatasetWriterReader<String> writerReader = dataset.writerReader();
    Spliterator<Record<String>> spliterator = streamStrategy.stream(writerReader).spliterator();

    AtomicInteger accessedElementCount = new AtomicInteger();
    Consumer<Record<String>> counter = r -> accessedElementCount.incrementAndGet();

    for (int i = 0; i < 2; i++) {
      // Force creation of an InlineElementSource and consume ONE element
      assertTrue(spliterator.tryAdvance(counter));

      for (ObservableDatasetActiveEntity<String> observableActiveEntity : observableActiveEntities) {
        for (PipelineProcessor processor : getPipelineProcessors(observableActiveEntity)) {
          @SuppressWarnings("unchecked") InlineElementSource<String, Record<String>> elementSource =
                  (InlineElementSource<String, Record<String>>) processor;

          /*
           * Obtain the server-side Spliterator supplying elements to the client-side stream above.
           */
          Spliterator<Record<String>> sourceSpliterator = elementSource.getSourceSpliterator();
          sourceSpliterator.forEachRemaining(counter);
        }
      }
    }

    assertThat(accessedElementCount.get(), is((int)streamStrategy.stream(writerReader).count()));
  }

  @Test
  public void testStreamId() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();

    RecordStream<String> animalStream = streamStrategy.stream(writerReader);

    AbstractShardedRecordStream<?, ?> shardedRecordStream = (AbstractShardedRecordStream<String, ?>) animalStream;

    for (RecordStream<?> s : shardedRecordStream.getInitialStreams()) {
      assertNull(((RemoteObject) s).getStreamId());   // Not yet open
    }

    Spliterator<Record<String>> spliterator = animalStream.spliterator();
    assertTrue(spliterator.tryAdvance(r -> {}));

    assertThat(shardedRecordStream.getInitialStreams().stream().map(s -> ((RemoteObject) s).getStreamId()).collect(toList()), hasItem(notNullValue()));
  }

  /**
   * Gets the sole {@link ObservableDatasetActiveEntity ObservableDatasetActiveEntity} from
   * {@link PassthroughTest#getDatasetEntityServerService()}.  This method may only be used <i>after</i>
   * a creation of a single dataset.
   *
   * @return the {@code ObservableDatasetActiveEntity} for the sole dataset
   */
  @SuppressWarnings("unchecked")
  //TODO: different in single-stripe mode
  private <K extends Comparable<K>> List<ObservableDatasetActiveEntity<K>> getObservableDatasetActiveEntity() {
    ObservableDatasetEntityServerService datasetEntityServerService = this.getDatasetEntityServerService();
    List<ObservableDatasetActiveEntity<? extends Comparable<?>>> activeEntities =
        datasetEntityServerService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(provideStripeNames().size()));
    return (List) activeEntities;   // unchecked
  }

  /**
   * Gets the {@link PipelineProcessor}s from the sole client connected to the {@link ObservableDatasetActiveEntity}
   * provided.
   *
   * @param activeEntity the {@code ObservableDatasetActiveEntity} from which the {@code PipelineProcessor}s
   *                     are obtained
   * @return the {@code PipelineProcessor}s for the active entity
   */
  private <K extends Comparable<K>>
  Collection<PipelineProcessor> getPipelineProcessors(ObservableDatasetActiveEntity<K> activeEntity) {
    Collection<ClientDescriptor> clientIds = activeEntity.getClientIds();
    assertThat(clientIds.size(), is(1));
    ClientDescriptor clientId = clientIds.iterator().next();
    return activeEntity.getOpenStreams(clientId);
  }

  @Override
  protected Dataset<String> getTestDataset(DatasetManager datasetManager) throws StoreException {
    datasetManager.newDataset("animals", Type.STRING, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build());
    Dataset<String> dataset = datasetManager.getDataset("animals", Type.STRING);
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    Animals.recordStream().forEach(r -> writerReader.add(r.getKey(), r));
    return dataset;
  }
}
