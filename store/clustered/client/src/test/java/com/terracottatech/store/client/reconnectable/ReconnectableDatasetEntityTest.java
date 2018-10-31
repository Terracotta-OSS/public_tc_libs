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
package com.terracottatech.store.client.reconnectable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.terracottatech.store.Cell;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.client.RecordImpl;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import com.terracottatech.store.intrinsics.impl.InstallOperation;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.store.stream.RecordStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Basic tests for {@link ReconnectableDatasetEntity}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ReconnectableDatasetEntityTest {

  @Mock
  private ReconnectController reconnectController;

  @Mock
  private DatasetEntity<String> mockEntity;

  private static final StringCellDefinition STRING_CELL = CellDefinition.defineString("stringCell");

  @Before
  public void setUp() {
    when(reconnectController.withConnectionClosedHandling(any()))
        .thenAnswer(invocation -> {
          Supplier<Object> supplier = invocation.getArgument(0);
          return supplier.get();
        });
    when(mockEntity.getKeyType()).thenReturn(Type.STRING);
  }

  @Test
  public void testConstruction() {
    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);

    assertNotNull(reconnectableDatasetEntity);
    assertThat(reconnectableDatasetEntity.getDelegate(), is(sameInstance(mockEntity)));
    verifyZeroInteractions(reconnectController);
  }

  @Test
  public void testClose() {
    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    reconnectableDatasetEntity.close();
    verify(mockEntity).close();
  }

  @Test
  public void testOnClose() {
    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);

    AtomicBoolean closeObserved = new AtomicBoolean();
    reconnectableDatasetEntity.onClose(() -> closeObserved.set(true));

    reconnectableDatasetEntity.close();

    assertTrue(closeObserved.get());

    try {
      reconnectableDatasetEntity.onClose(() -> {});
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      // expected
    }
  }


  @Test
  public void testGetKeyType() {
    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    assertThat(reconnectableDatasetEntity.getKeyType(), is(Type.STRING));
    verify(mockEntity).getKeyType();
    verify(reconnectController, never()).withConnectionClosedHandling(any());
  }

  @Test
  public void testRegisterDeregisterChangeListener() {
    @SuppressWarnings("unchecked") ArgumentCaptor<ChangeListener<String>> registrationCaptor =
        ArgumentCaptor.forClass(ChangeListener.class);
    doNothing().when(mockEntity).registerChangeListener(registrationCaptor.capture());

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);

    int listenerCount = 5;
    List<ChangeListener<String>> expectedListeners = new ArrayList<>();
    for (int i = 0; i < listenerCount; i++) {
      @SuppressWarnings("unchecked") ChangeListener<String> listener = mock(ChangeListener.class);
      reconnectableDatasetEntity.registerChangeListener(listener);
      expectedListeners.add(listener);
    }

    List<ChangeListener<String>> actualListeners = registrationCaptor.getAllValues();
    assertThat(actualListeners, hasSize(listenerCount));
    assertThat(actualListeners, containsInAnyOrder(expectedListeners.toArray()));
    verify(reconnectController, times(listenerCount)).withConnectionClosedHandling(any());

    for (ChangeListener<String> listener : expectedListeners) {
      verify(listener, never()).missedEvents();
    }

    @SuppressWarnings("unchecked") ArgumentCaptor<ChangeListener<String>> deregistrationCaptor =
        ArgumentCaptor.forClass(ChangeListener.class);
    doNothing().when(mockEntity).deregisterChangeListener(deregistrationCaptor.capture());

    for (ChangeListener<String> listener : expectedListeners) {
      reconnectableDatasetEntity.deregisterChangeListener(listener);
    }

    List<ChangeListener<String>> removedListeners = deregistrationCaptor.getAllValues();
    assertThat(removedListeners, hasSize(listenerCount));
    assertThat(removedListeners, containsInAnyOrder(expectedListeners.toArray()));
    verify(reconnectController, times(listenerCount * 2)).withConnectionClosedHandling(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAdd() {
    String key = "key";
    CellSet cells = CellSet.of(STRING_CELL.newCell("value"));

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<Iterable<Cell<?>>> cellsCaptor = ArgumentCaptor.forClass(Iterable.class);

    when(mockEntity.add(anyString(), any(Iterable.class))).thenReturn(true);

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    assertTrue(reconnectableDatasetEntity.add(key, cells));

    verify(mockEntity).add(keyCaptor.capture(), cellsCaptor.capture());
    assertThat(keyCaptor.getValue(), is(sameInstance(key)));
    assertThat(cellsCaptor.getValue(), is(sameInstance(cells)));
    verify(reconnectController).withConnectionClosedHandling(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAddReturnRecord() {
    String key = "key";
    CellSet cells = CellSet.of(STRING_CELL.newCell("value"));

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<Iterable<Cell<?>>> cellsCaptor = ArgumentCaptor.forClass(Iterable.class);

    when(mockEntity.addReturnRecord(anyString(), any(Iterable.class))).thenAnswer(
        (Answer<Record<String>>)invocation -> new RecordImpl<>(0L, invocation.getArgument(0), invocation.getArgument(1)));

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    Record<String> record = reconnectableDatasetEntity.addReturnRecord(key, cells);
    assertThat(record.getKey(), is(key));
    assertThat(record, containsInAnyOrder(cells.toArray()));

    verify(mockEntity).addReturnRecord(keyCaptor.capture(), cellsCaptor.capture());
    assertThat(keyCaptor.getValue(), is(sameInstance(key)));
    assertThat(cellsCaptor.getValue(), is(sameInstance(cells)));
    verify(reconnectController).withConnectionClosedHandling(any());
  }

  @Test
  public void testGet() {
    String key = "key";
    CellSet cells = CellSet.of(STRING_CELL.newCell("value"));

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);

    when(mockEntity.get(anyString())).thenAnswer(
        (Answer<Record<String>>)invocation -> new RecordImpl<>(0L, invocation.getArgument(0), cells));

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    Record<String> record = reconnectableDatasetEntity.get(key);
    assertThat(record.getKey(), is(key));
    assertThat(record, containsInAnyOrder(cells.toArray()));

    verify(mockEntity).get(keyCaptor.capture());
    assertThat(keyCaptor.getValue(), is(sameInstance(key)));
    verify(reconnectController).withConnectionClosedHandling(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGet2Arg() {
    String key = "key";
    CellSet cells = CellSet.of(STRING_CELL.newCell("value"));
    IntrinsicPredicate<? super Record<String>> predicate = AlwaysTrue.alwaysTrue();

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<IntrinsicPredicate<? super Record<String>>> predicateCaptor =
        ArgumentCaptor.forClass(IntrinsicPredicate.class);

    when(mockEntity.get(anyString(), any(IntrinsicPredicate.class))).thenAnswer(
        (Answer<Record<String>>)invocation -> new RecordImpl<>(0L, invocation.getArgument(0), cells));

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    Record<String> record = reconnectableDatasetEntity.get(key, predicate);
    assertThat(record.getKey(), is(key));
    assertThat(record, containsInAnyOrder(cells.toArray()));

    verify(mockEntity).get(keyCaptor.capture(), predicateCaptor.capture());
    assertThat(keyCaptor.getValue(), is(sameInstance(key)));
    assertThat(predicateCaptor.getValue(), is(sameInstance(predicate)));
    verify(reconnectController).withConnectionClosedHandling(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUpdate() {
    String key = "key";
    CellSet cells = CellSet.of(STRING_CELL.newCell("value"));
    IntrinsicPredicate<? super Record<String>> predicate = AlwaysTrue.alwaysTrue();
    IntrinsicUpdateOperation<String> transform = new InstallOperation<>(cells);

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<IntrinsicPredicate<? super Record<String>>> predicateCaptor =
        ArgumentCaptor.forClass(IntrinsicPredicate.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<IntrinsicUpdateOperation<String>> transformCaptor =
        ArgumentCaptor.forClass(IntrinsicUpdateOperation.class);

    when(mockEntity.update(anyString(), any(IntrinsicPredicate.class), any(IntrinsicUpdateOperation.class))).thenReturn(true);

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    boolean updated = reconnectableDatasetEntity.update(key, predicate, transform);
    assertTrue(updated);

    verify(mockEntity).update(keyCaptor.capture(), predicateCaptor.capture(), transformCaptor.capture());
    assertThat(keyCaptor.getValue(), is(sameInstance(key)));
    assertThat(predicateCaptor.getValue(), is(sameInstance(predicate)));
    assertThat(transformCaptor.getValue(), is(sameInstance(transform)));
    verify(reconnectController).withConnectionClosedHandling(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUpdateReturnTuple() {
    String key = "key";
    CellSet cells = CellSet.of(STRING_CELL.newCell("value"));
    IntrinsicPredicate<? super Record<String>> predicate = AlwaysTrue.alwaysTrue();
    IntrinsicUpdateOperation<String> transform = new InstallOperation<>(cells);

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<IntrinsicPredicate<? super Record<String>>> predicateCaptor =
        ArgumentCaptor.forClass(IntrinsicPredicate.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<IntrinsicUpdateOperation<String>> transformCaptor =
        ArgumentCaptor.forClass(IntrinsicUpdateOperation.class);

    when(mockEntity.updateReturnTuple(anyString(), any(IntrinsicPredicate.class), any(IntrinsicUpdateOperation.class)))
        .thenAnswer(invocation -> Tuple.of(
            new RecordImpl<>(0L, invocation.getArgument(0), cells),
            new RecordImpl<>(1L, invocation.getArgument(0), cells)
        ));

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    Tuple<Record<String>, Record<String>> tuple = reconnectableDatasetEntity.updateReturnTuple(key, predicate, transform);
    assertThat(tuple.getFirst().getKey(), is(key));
    assertThat(tuple.getFirst(), containsInAnyOrder(cells.toArray()));
    assertThat(tuple.getSecond().getKey(), is(key));
    assertThat(tuple.getSecond(), containsInAnyOrder(cells.toArray()));

    verify(mockEntity).updateReturnTuple(keyCaptor.capture(), predicateCaptor.capture(), transformCaptor.capture());
    assertThat(keyCaptor.getValue(), is(sameInstance(key)));
    assertThat(predicateCaptor.getValue(), is(sameInstance(predicate)));
    assertThat(transformCaptor.getValue(), is(sameInstance(transform)));
    verify(reconnectController).withConnectionClosedHandling(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDelete() {
    String key = "key";
    IntrinsicPredicate<? super Record<String>> predicate = AlwaysTrue.alwaysTrue();

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<IntrinsicPredicate<? super Record<String>>> predicateCaptor =
        ArgumentCaptor.forClass(IntrinsicPredicate.class);

    when(mockEntity.delete(anyString(), any(IntrinsicPredicate.class))).thenReturn(true);

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    boolean deleted = reconnectableDatasetEntity.delete(key, predicate);
    assertTrue(deleted);

    verify(mockEntity).delete(keyCaptor.capture(), predicateCaptor.capture());
    assertThat(keyCaptor.getValue(), is(sameInstance(key)));
    assertThat(predicateCaptor.getValue(), is(sameInstance(predicate)));
    verify(reconnectController).withConnectionClosedHandling(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteReturnRecord() {
    String key = "key";
    CellSet cells = CellSet.of(STRING_CELL.newCell("value"));
    IntrinsicPredicate<? super Record<String>> predicate = AlwaysTrue.alwaysTrue();

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<IntrinsicPredicate<? super Record<String>>> predicateCaptor =
        ArgumentCaptor.forClass(IntrinsicPredicate.class);

    when(mockEntity.deleteReturnRecord(anyString(), any(IntrinsicPredicate.class))).thenAnswer(
        (Answer<Record<String>>)invocation -> new RecordImpl<>(0L, invocation.getArgument(0), cells));

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    Record<String> record = reconnectableDatasetEntity.deleteReturnRecord(key, predicate);
    assertThat(record.getKey(), is(key));
    assertThat(record, containsInAnyOrder(cells.toArray()));

    verify(mockEntity).deleteReturnRecord(keyCaptor.capture(), predicateCaptor.capture());
    assertThat(keyCaptor.getValue(), is(sameInstance(key)));
    assertThat(predicateCaptor.getValue(), is(sameInstance(predicate)));
    verify(reconnectController).withConnectionClosedHandling(any());
  }

  @Test
  public void testGetIndexing() {
    Indexing mockIndexing = mock(Indexing.class);
    when(mockEntity.getIndexing()).thenReturn(mockIndexing);

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    Indexing indexing = reconnectableDatasetEntity.getIndexing();
    assertThat(indexing, is(instanceOf(ReconnectableIndexing.class)));
    verify(reconnectController).withConnectionClosedHandling(any());
  }

  @Test
  public void testNonMutableStream() {
    @SuppressWarnings("unchecked") RecordStream<String> mockStream = mock(RecordStream.class);
    when(mockEntity.nonMutableStream()).thenReturn(mockStream);

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    RecordStream<String> stream = reconnectableDatasetEntity.nonMutableStream();
    assertThat(stream, is(sameInstance(mockStream)));
    verify(reconnectController).withConnectionClosedHandling(any());
  }

  @Test
  public void testMutableStream() {
    @SuppressWarnings("unchecked") MutableRecordStream<String> mockStream = mock(MutableRecordStream.class);
    when(mockEntity.mutableStream()).thenReturn(mockStream);

    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);
    RecordStream<String> stream = reconnectableDatasetEntity.mutableStream();
    assertThat(stream, is(sameInstance(mockStream)));
    verify(reconnectController).withConnectionClosedHandling(any());
  }

  @Test
  public void testSwap() {

    @SuppressWarnings("unchecked") ArgumentCaptor<ChangeListener<String>> deregistrationCaptor =
        ArgumentCaptor.forClass(ChangeListener.class);
    doNothing().when(mockEntity).deregisterChangeListener(deregistrationCaptor.capture());

    /*
     * Create DatasetEntity using first delegate
     */
    ReconnectableDatasetEntity<String> reconnectableDatasetEntity = new ReconnectableDatasetEntity<>(mockEntity, reconnectController);

    int listenerCount = 5;
    List<ChangeListener<String>> expectedListeners = new ArrayList<>();
    for (int i = 0; i < listenerCount; i++) {
      @SuppressWarnings("unchecked") ChangeListener<String> listener = mock(ChangeListener.class);
      reconnectableDatasetEntity.registerChangeListener(listener);
      expectedListeners.add(listener);
    }

    int indexingCount = 3;
    List<Indexing> observedMockIndexings = new ArrayList<>();
    when(mockEntity.getIndexing()).then((Answer<Indexing>)invocation -> {
      Indexing mockIndexing = mock(Indexing.class);
      observedMockIndexings.add(mockIndexing);
      return mockIndexing;
    });

    List<ReconnectableIndexing> reconnectableIndexings = new ArrayList<>();
    for (int i = 0; i < indexingCount; i++) {
      reconnectableIndexings.add(((ReconnectableIndexing)reconnectableDatasetEntity.getIndexing()));
    }

    for (Indexing indexing : observedMockIndexings) {
      verifyZeroInteractions(indexing);
    }

    observedMockIndexings.clear();

    /*
     * Prepare replacement delegate ...
     */
    @SuppressWarnings("unchecked") DatasetEntity<String> secondDelegate = mock(DatasetEntity.class);
    when(secondDelegate.getKeyType()).thenReturn(Type.STRING);

    @SuppressWarnings("unchecked") ArgumentCaptor<ChangeListener<String>> secondRegistrationCaptor =
        ArgumentCaptor.forClass(ChangeListener.class);
    doNothing().when(secondDelegate).registerChangeListener(secondRegistrationCaptor.capture());

    when(secondDelegate.getIndexing()).then((Answer<Indexing>)invocation -> {
      Indexing mockIndexing = mock(Indexing.class);
      observedMockIndexings.add(mockIndexing);
      return mockIndexing;
    });

    /*
     * Swap in second delegate ...
     */
    reconnectableDatasetEntity.swap(secondDelegate);

    assertThat(reconnectableDatasetEntity.getDelegate(), is(sameInstance(secondDelegate)));

    /*
     * Ensure listeners were transferred
     */
    List<ChangeListener<String>> firstDeregisteredListeners = deregistrationCaptor.getAllValues();
    List<ChangeListener<String>> secondRegisteredListeners = secondRegistrationCaptor.getAllValues();
    assertThat(firstDeregisteredListeners, hasSize(listenerCount));
    assertThat(firstDeregisteredListeners, containsInAnyOrder(expectedListeners.toArray()));
    assertThat(secondRegisteredListeners, containsInAnyOrder(expectedListeners.toArray()));

    for (ChangeListener<String> listener : expectedListeners) {
      verify(listener).missedEvents();
    }

    /*
     * Ensure the Indexing instances were updated
     */
    assertThat(observedMockIndexings, hasSize(indexingCount));
    for (Indexing indexing : observedMockIndexings) {
      verify(indexing).getAllIndexes();
    }
  }
}