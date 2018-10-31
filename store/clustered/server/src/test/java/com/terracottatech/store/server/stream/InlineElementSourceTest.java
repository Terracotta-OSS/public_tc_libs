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
package com.terracottatech.store.server.stream;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignDataset.Durability;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.messages.stream.NonPortableTransform;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchMessage;
import com.terracottatech.store.common.messages.stream.inline.WaypointMarker;
import com.terracottatech.store.server.execution.PipelineProcessorExecutor;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.terracotta.entity.ClientDescriptor;

import com.terracottatech.store.Record;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.common.messages.stream.ElementType;
import com.terracottatech.test.data.Animals;
import com.terracottatech.test.data.Animals.AnimalRecord;
import com.terracottatech.tool.Diagnostics;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.IEntityMessenger;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.DELETE_THEN;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.FILTER;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MUTATE_THEN;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MUTATE_THEN_INTERNAL;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.DELETE;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.MUTATE;
import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests basic functionality of {@link InlineElementSource}.
 */
public class InlineElementSourceTest {

  @Mock
  private ClientDescriptor client;

  @Mock
  private SovereignDataset<String> dataset;

  @Captor
  private ArgumentCaptor<Function<Record<String>, Object>> deleteFunctionCaptor;
  @Captor
  private ArgumentCaptor<Function<Record<String>, Iterable<Cell<?>>>> transformCaptor;
  @Captor
  private ArgumentCaptor<BiFunction<Record<String>, Record<String>, Object>> mapperCaptor;

  private ExecutorService executor;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(client.toString()).thenReturn("Client{test}");

    when(dataset.getAlias()).thenReturn("datasetName");
    when(dataset.records())
        .thenAnswer(invocation -> new TestRecordStream<>(Animals.recordStream()));

    /* For delete(Durability, Function), simply return the function. */
    when(dataset.delete(any(Durability.class), deleteFunctionCaptor.capture()))
        .thenAnswer(invocation -> invocation.<Function<Record<String>, Object>>getArgument(1));

    /* For applyMutation(Durability, Function, BiFunction), return ... */
    when(dataset.applyMutation(any(Durability.class), transformCaptor.capture(), mapperCaptor.capture()))
        .thenAnswer((Answer<Function<Record<String>, Object>>)invocation -> {
          Function<Record<String>, Iterable<Cell<?>>> transform = invocation.getArgument(1);
          BiFunction<Record<String>, Record<String>, Object> mapper = invocation.getArgument(2);
          return record -> mapper.apply(record, new AnimalRecord(record.getKey(), transform.apply(record)));
        });

    executor = new PipelineProcessorExecutor("test", 500, TimeUnit.MILLISECONDS, 128);
  }

  @After
  public void tearDown() throws Exception {
    executor.shutdownNow();
  }

  @Test(timeout = 2000)
  public void testBasics() throws Exception {
    InlineElementSource<String, Record<String>> elementSource = startElementSource();

    List<Record<String>> records = new ArrayList<>();
    withDiagnosticTimer(2000, () -> {
      Record<String> record;
      while ((record = fetchRecord(elementSource)) != null) {
        records.add(record);
        elementSource.release();
      }

      assertThat(elementSource.fetch(), is(nullValue()));

      elementSource.close();

      assertThrows(elementSource::fetch, StoreRuntimeException.class);
    });
    assertThat(records, IsIterableContainingInOrder.contains(Animals.recordStream().toArray()));
  }

  /**
   * Ensures the mutation operations all mark the {@link InlineElementSource} as blockable.
   */
  @Test
  public void testBlockable() throws Exception {
    for (List<PipelineOperation> portableOperations : asList(
        singletonList(MUTATE_THEN.newInstance(UpdateOperation.install())),
        singletonList(DELETE_THEN.newInstance())
    )) {
      IEntityMessenger<EntityMessage, ?> m = mockIEntityMessenger();
      InlineElementSource<String, Record<String>> elementSource =
          new InlineElementSource<>(executor, m, client, UUID.randomUUID(), ElementType.RECORD, dataset, portableOperations, false);
      assertTrue("ElementSource.isBlocking()==false for " + portableOperations, elementSource.isBlocking(new TryAdvanceFetchMessage(null)));
    }

    class TerminatedPipeline {
      private final List<PipelineOperation> intermediateOperations;
      private final PipelineOperation terminalOperation;

      private TerminatedPipeline(List<PipelineOperation> intermediateOperations, PipelineOperation terminalOperation) {
        this.intermediateOperations = intermediateOperations;
        this.terminalOperation = terminalOperation;
      }
    }

    for (TerminatedPipeline descriptor : asList(
        new TerminatedPipeline(singletonList(FILTER.newInstance(new WaypointMarker<>(0))),
            MUTATE.newInstance(UpdateOperation.install())),
        new TerminatedPipeline(emptyList(), DELETE.newInstance())
    )) {
      PipelineOperation terminalOperation = descriptor.terminalOperation;
      InlineElementSource<String, Record<String>> elementSource =
          new InlineElementSource<>(executor, mockIEntityMessenger(), client, UUID.randomUUID(), ElementType.RECORD, dataset,
              descriptor.intermediateOperations, terminalOperation, false);
      assertTrue("ElementSource.isBlocking()==false for " + terminalOperation, elementSource.isBlocking(new TryAdvanceFetchMessage(null)));
    }
  }

  @Test(timeout = 1500)
  public void testCloseBeforeFetch() throws Exception {
    InlineElementSource<String, Record<String>> elementSource = startElementSource();

    withDiagnosticTimer(1000, () -> {
      elementSource.close();
      assertThrows(elementSource::fetch, StoreRuntimeException.class);
      try {
        elementSource.release();
      } catch (Exception e) {
        throw new AssertionError("InlineElementSource.release threw after close", e);
      }
      try {
        elementSource.close();
      } catch (Exception e) {
        throw new AssertionError("InlineElementSource.close threw after close", e);
      }
    });
  }

  @Test(timeout = 1500)
  public void testCloseBeforeRelease() throws Exception {
    InlineElementSource<String, Record<String>> elementSource = startElementSource();

    withDiagnosticTimer(1000, () -> {
      Record<String> record = fetchRecord(elementSource);
      assertThat(record, is(notNullValue()));
      elementSource.close();

      try {
        elementSource.release();
      } catch (Exception e) {
        throw new AssertionError("InlineElementSource.release threw after close", e);
      }

      assertThrows(elementSource::fetch, StoreRuntimeException.class);

      try {
        elementSource.close();
      } catch (Exception e) {
        throw new AssertionError("InlineElementSource.close threw after close", e);
      }
    });
  }

  @Test(timeout = 1500)
  public void testCloseDuringFetch() throws Exception {
    AtomicReference<InlineElementSource<?, ?>> elementSourceReference = new AtomicReference<>();
    AtomicInteger counter = new AtomicInteger(5);
    Stream<Record<String>> baseStream = Animals.recordStream();
    Stream<Record<String>> recordStream = baseStream.peek(r -> {
      if (counter.get() == 0) {
        /*
         * This closure is during fetch of the 6th element _in the background thread_.
         */
        elementSourceReference.get().close();
      }
    });
    InlineElementSource<String, Record<String>> elementSource =
        new InlineElementSource<>(executor, mockIEntityMessenger(), dataset, client, UUID.randomUUID(), ElementType.RECORD, recordStream);
    elementSourceReference.set(elementSource);
    executor.execute(elementSource);

    withDiagnosticTimer(1000, () -> {
      for (int i = counter.get(); i > 0; i--) {
        Record<String> record = fetchRecord(elementSource);
        assertThat(record, is(notNullValue()));
        counter.decrementAndGet();
        elementSource.release();
      }

      assertThrows(elementSource::fetch, StoreRuntimeException.class);
    });
  }

  @Test
  public void testPortableMutateThenInternalWithoutWaypoint() throws Exception {
    List<PipelineOperation> intermediateOperations = asList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal")),
        MUTATE_THEN_INTERNAL.newInstance(
            UpdateOperation.write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment()))
    );
    assertThrows(() -> startElementSource(ElementType.RECORD, intermediateOperations), IllegalStateException.class);
  }

  @Test(timeout = 3000)
  public void testPortableMutateThenInternal() throws Exception {
    List<PipelineOperation> intermediateOperations = asList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal")),
        FILTER.newInstance(new WaypointMarker<>(0)),
        MUTATE_THEN_INTERNAL.newInstance(
            UpdateOperation.write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment()))
    );
    InlineElementSource<String, Object> elementSource = startElementSource(ElementType.RECORD, intermediateOperations);

    List<String> observedKeys = new ArrayList<>();
    withDiagnosticTimer(2000, () -> {
      do {
        assertThat(elementSource.getCurrentState(), is("READY"));

        /*
         * Fetch when waypoint is in use ends with a WaypointRequest (or stream exhaustion).
         */
        Object fetchResult = elementSource.fetch();
        if (fetchResult == null) {
          assertThat(elementSource.getCurrentState(), is("DRAINED"));
          break;
        }
        assertThat(fetchResult, is(instanceOf(WaypointRequest.class)));
        assertThat(elementSource.getCurrentState(), is("WAYPOINT_PENDING"));
        @SuppressWarnings("unchecked") WaypointRequest<Record<String>> waypointRequest =
            (WaypointRequest<Record<String>>)fetchResult;
        Record<String> waypointRecord = waypointRequest.getPendingElement();
        assertThat(waypointRecord, is(instanceOf(Record.class)));
        observedKeys.add(waypointRecord.getKey());

        /*
         * Simulate the presence of a filter(r -> !r.getKey().equals("echidna")) in the client pipeline.
         * This results in a release() being issued for the element (to drop the "echidna" record) instead
         * of a continueFetch() call.
         */
        if (!waypointRecord.getKey().equals("echidna")) {
          /*
           * Not an echidna ... resume pipeline with a fetchContinue.  Since the mutation transform is
           * portable, the fetchContinue has no payload.  Because the pipeline ends with a
           * MUTATE_THEN_INTERNAL, the result of fetchContinue is a Record -- the new one.
           */
          fetchResult = elementSource.continueFetch(waypointRequest.getWaypointId(), null);
          assertThat(fetchResult, is(instanceOf(Record.class)));
          assertThat(elementSource.getCurrentState(), is("RELEASE_REQUIRED"));

          @SuppressWarnings("unchecked") Record<String> fetchRecord = (Record<String>)fetchResult;
          assertThat(fetchRecord.getKey(), is(waypointRecord.getKey()));

          for (Cell<?> cell : fetchRecord) {
            if (cell.definition().equals(OBSERVATIONS)) {
              assertThat(cell.value(), is(waypointRecord.get(OBSERVATIONS).orElse(0L) + 1));
            } else {
              assertThat(cell.value(), is(waypointRecord.get(cell.definition()).orElseThrow(AssertionError::new)));
            }
          }
        }

        /*
         * Release the pipeline to enable processing the next element;
         */
        elementSource.release();
      } while (true);

      elementSource.close();
      assertThat(elementSource.getCurrentState(), is("CLOSED"));

      assertThat(observedKeys,
          contains(Animals.recordStream().filter(TAXONOMIC_CLASS.value().is("mammal")).map(Record::getKey).toArray()));
    });
  }

  @Test
  public void testNonPortableMutateThenInternalWithoutWaypoint() throws Exception {
    List<PipelineOperation> intermediateOperations = asList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal")),
        MUTATE_THEN_INTERNAL.newInstance(new NonPortableTransform<>(0))
    );
    assertThrows(() -> startElementSource(ElementType.RECORD, intermediateOperations), IllegalStateException.class);
  }

  @Test(timeout = 3000)
  public void testNonPortableMutateThenInternal() throws Exception {
    UpdateOperation.CellUpdateOperation<String, Long> transform =
        UpdateOperation.write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment());
    List<PipelineOperation> intermediateOperations = asList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal")),
        FILTER.newInstance(new WaypointMarker<>(0)),
        MUTATE_THEN_INTERNAL.newInstance(new NonPortableTransform<>(0))
    );
    InlineElementSource<String, Object> elementSource = startElementSource(ElementType.RECORD, intermediateOperations);

    List<String> observedKeys = new ArrayList<>();
    withDiagnosticTimer(2000, () -> {
      do {
        assertThat(elementSource.getCurrentState(), is("READY"));

        /*
         * Fetch when waypoint is in use ends with a WaypointRequest (or stream exhaustion).
         */
        Object fetchResult = elementSource.fetch();
        if (fetchResult == null) {
          assertThat(elementSource.getCurrentState(), is("DRAINED"));
          break;
        }
        assertThat(fetchResult, is(instanceOf(WaypointRequest.class)));
        assertThat(elementSource.getCurrentState(), is("WAYPOINT_PENDING"));
        @SuppressWarnings("unchecked") WaypointRequest<Record<String>> waypointRequest =
            (WaypointRequest<Record<String>>)fetchResult;
        Record<String> waypointRecord = waypointRequest.getPendingElement();
        assertThat(waypointRecord, is(instanceOf(Record.class)));
        observedKeys.add(waypointRecord.getKey());

        /*
         * Simulate the presence of a filter(r -> !r.getKey().equals("echidna")) in the client pipeline.
         * This results in a release() being issued for the element (to drop the "echidna" record) instead
         * of a continueFetch() call.
         */
        if (!waypointRecord.getKey().equals("echidna")) {
          /*
           * Not an echidna ... resume pipeline with a fetchContinue.  we're simulating a non-portable
           * transform so fetchContinue needs a payload.  Because the pipeline ends with a
           * MUTATE_THEN_INTERNAL, the result of fetchContinue is a Record -- the new one.
           */
          fetchResult = elementSource.continueFetch(waypointRequest.getWaypointId(), transform.apply(waypointRecord));
          assertThat(fetchResult, is(instanceOf(Record.class)));
          assertThat(elementSource.getCurrentState(), is("RELEASE_REQUIRED"));

          @SuppressWarnings("unchecked") Record<String> fetchRecord = (Record<String>)fetchResult;
          assertThat(fetchRecord.getKey(), is(waypointRecord.getKey()));

          for (Cell<?> cell : fetchRecord) {
            if (cell.definition().equals(OBSERVATIONS)) {
              assertThat(cell.value(), is(waypointRecord.get(OBSERVATIONS).orElse(0L) + 1));
            } else {
              assertThat(cell.value(), is(waypointRecord.get(cell.definition()).orElseThrow(AssertionError::new)));
            }
          }
        }

        /*
         * Release the pipeline to enable processing the next element;
         */
        elementSource.release();
      } while (true);

      elementSource.close();
      assertThat(elementSource.getCurrentState(), is("CLOSED"));

      assertThat(observedKeys,
          contains(Animals.recordStream().filter(TAXONOMIC_CLASS.value().is("mammal")).map(Record::getKey).toArray()));
    });
  }

  @Test(timeout = 3000)
  public void testMutateThenWithoutWaypoint() throws Exception {
    List<PipelineOperation> intermediateOperations = asList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal")),
        MUTATE_THEN.newInstance(UpdateOperation.write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment()))
    );
    InlineElementSource<String, Object> elementSource = startElementSource(ElementType.RECORD, intermediateOperations);

    List<String> observedKeys = new ArrayList<>();
    withDiagnosticTimer(2000, () -> {
      do {
        assertThat(elementSource.getCurrentState(), is("READY"));

        /*
         * Fetch when waypoint is in use ends with a WaypointRequest (or stream exhaustion).
         */
        Object fetchResult = elementSource.fetch();
        if (fetchResult == null) {
          assertThat(elementSource.getCurrentState(), is("DRAINED"));
          break;
        }
        assertThat(elementSource.getCurrentState(), is("RELEASE_REQUIRED"));
        assertThat(fetchResult, is(instanceOf(Tuple.class)));
        assertThat(((Tuple)fetchResult).getFirst(), is(instanceOf(Record.class)));
        assertThat(((Tuple)fetchResult).getSecond(), is(instanceOf(Record.class)));
        @SuppressWarnings("unchecked") Tuple<Record<String>, Record<String>> tuple =
            (Tuple<Record<String>, Record<String>>)fetchResult;

        Record<String> originalRecord = tuple.getFirst();
        assertThat(originalRecord.getKey(), is(tuple.getSecond().getKey()));

        for (Cell<?> cell : tuple.getSecond()) {
          if (cell.definition().equals(OBSERVATIONS)) {
            assertThat(cell.value(), is(originalRecord.get(OBSERVATIONS).orElse(0L) + 1));
          } else {
            assertThat(cell.value(), is(originalRecord.get(cell.definition()).orElseThrow(AssertionError::new)));
          }
        }
        observedKeys.add(originalRecord.getKey());

        /*
         * Release the pipeline to enable processing the next element;
         */
        elementSource.release();
      } while (true);

      elementSource.close();
      assertThat(elementSource.getCurrentState(), is("CLOSED"));

      assertThat(observedKeys,
          contains(Animals.recordStream().filter(TAXONOMIC_CLASS.value().is("mammal")).map(Record::getKey).toArray()));
    });
  }

  @Test
  public void testPortableMutateWithoutWaypoint() throws Exception {
    List<PipelineOperation> intermediateOperations = singletonList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal"))
    );
    PipelineOperation terminalOperation =
        MUTATE.newInstance(UpdateOperation.write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment()));
    assertThrows(() -> startElementSource(ElementType.ELEMENT_VALUE, intermediateOperations, terminalOperation),
        IllegalStateException.class);
  }

  @Test(timeout = 3000)
  public void testPortableMutate() throws Exception {
    List<PipelineOperation> intermediateOperations = asList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal")),
        FILTER.newInstance(new WaypointMarker<>(0))
    );
    PipelineOperation terminalOperation =
        MUTATE.newInstance(UpdateOperation.write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment()));
    InlineElementSource<String, Object> elementSource =
        startElementSource(ElementType.RECORD, intermediateOperations, terminalOperation);

    List<String> observedKeys = new ArrayList<>();
    withDiagnosticTimer(2000, () -> {
      do {
        assertThat(elementSource.getCurrentState(), is("READY"));

        /*
         * Fetch when waypoint is in use ends with a WaypointRequest (or stream exhaustion).
         */
        Object fetchResult = elementSource.fetch();
        if (fetchResult == null) {
          assertThat(elementSource.getCurrentState(), is("DRAINED"));
          break;
        }
        assertThat(fetchResult, is(instanceOf(WaypointRequest.class)));
        assertThat(elementSource.getCurrentState(), is("WAYPOINT_PENDING"));
        @SuppressWarnings("unchecked") WaypointRequest<Record<String>> waypointRequest =
            (WaypointRequest<Record<String>>)fetchResult;
        Record<String> waypointRecord = waypointRequest.getPendingElement();
        assertThat(waypointRecord, is(instanceOf(Record.class)));
        observedKeys.add(waypointRecord.getKey());

        /*
         * Simulate the presence of a filter(r -> !r.getKey().equals("echidna")) in the client pipeline.
         * This results in a release() being issued for the element (to drop the "echidna" record) instead
         * of a continueFetch() call.
         */
        if (!waypointRecord.getKey().equals("echidna")) {
          /*
           * Not an echidna ... resume pipeline with a fetchContinue.  Since the mutation transform is
           * portable, the fetchContinue has no payload.  Because the pipeline ends with a
           * MUTATE, the result of fetchContinue is a NonElement.CONSUMED.
           */
          fetchResult = elementSource.continueFetch(waypointRequest.getWaypointId(), null);
          assertThat(fetchResult, is(NonElement.CONSUMED));
          assertThat(elementSource.getCurrentState(), is("READY"));
        } else {
          /*
           * Release is required to "drop" element.
           */
          elementSource.release();
        }
      } while (true);

      elementSource.close();
      assertThat(elementSource.getCurrentState(), is("CLOSED"));

      assertThat(observedKeys,
          contains(Animals.recordStream().filter(TAXONOMIC_CLASS.value().is("mammal")).map(Record::getKey).toArray()));
    });
  }

  @Test
  public void testNonPortableMutateWithoutWaypoint() throws Exception {
    List<PipelineOperation> intermediateOperations = singletonList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal"))
    );
    PipelineOperation terminalOperation = MUTATE.newInstance(new NonPortableTransform<>(0));
    assertThrows(() -> startElementSource(ElementType.ELEMENT_VALUE, intermediateOperations, terminalOperation),
        IllegalStateException.class);
  }

  @Test(timeout = 3000)
  public void testNonPortableMutate() throws Exception {
    UpdateOperation.CellUpdateOperation<String, Long> transform =
        UpdateOperation.write(OBSERVATIONS).longResultOf(OBSERVATIONS.longValueOr(0L).increment());
    List<PipelineOperation> intermediateOperations = asList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal")),
        FILTER.newInstance(new WaypointMarker<>(0))
    );
    PipelineOperation terminalOperation = MUTATE.newInstance(new NonPortableTransform<>(0));
    InlineElementSource<String, Object> elementSource =
        startElementSource(ElementType.RECORD, intermediateOperations, terminalOperation);

    List<String> observedKeys = new ArrayList<>();
    withDiagnosticTimer(2000, () -> {
      do {
        assertThat(elementSource.getCurrentState(), is("READY"));

        /*
         * Fetch when waypoint is in use ends with a WaypointRequest (or stream exhaustion).
         */
        Object fetchResult = elementSource.fetch();
        if (fetchResult == null) {
          assertThat(elementSource.getCurrentState(), is("DRAINED"));
          break;
        }
        assertThat(fetchResult, is(instanceOf(WaypointRequest.class)));
        assertThat(elementSource.getCurrentState(), is("WAYPOINT_PENDING"));
        @SuppressWarnings("unchecked") WaypointRequest<Record<String>> waypointRequest =
            (WaypointRequest<Record<String>>)fetchResult;
        Record<String> waypointRecord = waypointRequest.getPendingElement();
        assertThat(waypointRecord, is(instanceOf(Record.class)));
        observedKeys.add(waypointRecord.getKey());

        /*
         * Simulate the presence of a filter(r -> !r.getKey().equals("echidna")) in the client pipeline.
         * This results in a release() being issued for the element (to drop the "echidna" record) instead
         * of a continueFetch() call.
         */
        if (!waypointRecord.getKey().equals("echidna")) {
          /*
           * Not an echidna ... resume pipeline with a fetchContinue.  we're simulating a non-portable
           * transform so fetchContinue needs a payload.  Because the pipeline ends with a
           * MUTATE, the result of fetchContinue is a NonElement.CONSUMED.
           */
          fetchResult = elementSource.continueFetch(waypointRequest.getWaypointId(), transform.apply(waypointRecord));
          assertThat(fetchResult, is(NonElement.CONSUMED));
          assertThat(elementSource.getCurrentState(), is("READY"));
        } else {
          /*
           * Release is required to "drop" element.
           */
          elementSource.release();
        }
      } while (true);

      elementSource.close();
      assertThat(elementSource.getCurrentState(), is("CLOSED"));

      assertThat(observedKeys,
          contains(Animals.recordStream().filter(TAXONOMIC_CLASS.value().is("mammal")).map(Record::getKey).toArray()));
    });
  }

  private void withDiagnosticTimer(int delay, Procedure proc) {
    Timer timer = new Timer(true);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        Diagnostics.threadDump();
      }
    }, delay);

    try {
      proc.invoke();
    } finally {
      timer.cancel();
    }
  }

  private InlineElementSource<String, Record<String>> startElementSource() {
    InlineElementSource<String, Record<String>> elementSource =
        new InlineElementSource<>(executor, mockIEntityMessenger(), dataset, client, UUID.randomUUID(), ElementType.RECORD, Animals.recordStream());

    executor.execute(elementSource);
    Thread.yield();
    return elementSource;
  }

  private <T> InlineElementSource<String, T> startElementSource(ElementType elementType,
                                                                List<PipelineOperation> intermediateOperations) {
    InlineElementSource<String, T> elementSource =
        new InlineElementSource<>(executor, mockIEntityMessenger(), client, UUID.randomUUID(), elementType, dataset, intermediateOperations, false);

    executor.execute(elementSource);
    Thread.yield();
    return elementSource;
  }

  private <T> InlineElementSource<String, T> startElementSource(ElementType elementType,
                                                                List<PipelineOperation> intermediateOperations,
                                                                PipelineOperation terminalOperation) {
    InlineElementSource<String, T> elementSource =
        new InlineElementSource<>(executor, mockIEntityMessenger(), client, UUID.randomUUID(), elementType, dataset, intermediateOperations, terminalOperation, false);

    executor.execute(elementSource);
    Thread.yield();
    return elementSource;
  }

  @SuppressWarnings("Duplicates")
  private static <T extends Exception> void assertThrows(Procedure proc, Class<T> expected) {
    try {
      proc.invoke();
      fail("Expecting " + expected.getSimpleName());
    } catch (Exception t) {
      if (!expected.isInstance(t)) {
        throw t;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Record<String> fetchRecord(InlineElementSource<String, Record<String>> elementSource) {
    return (Record<String>) elementSource.fetch();
  }

  @SuppressWarnings("unchecked")
  private IEntityMessenger<EntityMessage, ?> mockIEntityMessenger() {
    return mock(IEntityMessenger.class);
  }

  @FunctionalInterface
  private interface Procedure {
    void invoke();
  }
}