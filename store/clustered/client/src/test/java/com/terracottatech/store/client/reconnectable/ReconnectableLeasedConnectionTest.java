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

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.terracotta.catalog.SystemCatalog;
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.lease.connection.LeasedConnection;

import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.client.reconnectable.ReconnectableLeasedConnection.ConnectionSupplier;
import org.apache.log4j.LogManager;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.*;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Basic tests for {@link ReconnectableLeasedConnection}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ReconnectableLeasedConnectionTest {

  @Captor
  private ArgumentCaptor<Class<? extends Entity>> classCaptor;

  @Captor
  private ArgumentCaptor<Long> longCaptor;

  @Captor
  private ArgumentCaptor<String> stringCaptor;

  @Test
  public void testConstruction() throws Exception {
    AtomicReference<LeasedConnection> observedConnection = new AtomicReference<>();
    ConnectionSupplier connectionSupplier = mock(ConnectionSupplier.class);
    when(connectionSupplier.get()).then(
        (Answer<LeasedConnection>)invocationOnMock -> ReconnectableLeasedConnectionTest.this.getConnection(observedConnection::set));

    @SuppressWarnings("unused")
    ReconnectableLeasedConnection reconnectableConnection = new ReconnectableLeasedConnection(connectionSupplier, 0L);

    verify(connectionSupplier).get();
  }

  @Test
  public void testEntityRefDataset() throws Exception {
    testEntityRef(ReconnectableDatasetEntityRef.class, DatasetEntity.class, 0L, "datasetName");
  }

  @Test
  public void testEntityRefGeneric() throws Exception {
    testEntityRef(ReconnectableEntityRef.class, SystemCatalog.class, SystemCatalog.VERSION, SystemCatalog.ENTITY_NAME);
  }

  @Test
  public void testReconnectTimeout() throws Exception {
    ConnectionSupplier connectionSupplier = mock(ConnectionSupplier.class);

    AtomicReference<LeasedConnection> observedConnection = new AtomicReference<>();
    Queue<GetConnectionAction> getConnectionActions = new ConcurrentLinkedQueue<>();
    AtomicReference<Queue<GetConnectionAction>> actionQueue = new AtomicReference<>(getConnectionActions);
    List<EntityRef<SystemCatalog, Object, Object>> deliveredEntityRefs = new ArrayList<>();
    when(connectionSupplier.get()).thenAnswer(
        (Answer<LeasedConnection>)invocationOnMock -> getLeasedConnection(observedConnection, deliveredEntityRefs, actionQueue.get()));

    ReconnectableLeasedConnection reconnectableConnection =
        new ReconnectableLeasedConnection(connectionSupplier, TimeUnit.SECONDS.toMillis(5L));
    verify(connectionSupplier, times(1)).get();
    assertThat(observedConnection.get(), is(notNullValue()));

    /*
     * Initiate a reconnect ... expected to ultimately fail with a TimeoutException
     */
    actionQueue.set(new RepeatingQueue<>(GetConnectionAction.RAISE));
    observedConnection.set(null);
    try {
      reconnectableConnection.reconnect();
      fail("Expecting TimeoutException");
    } catch (TimeoutException e) {
      assertThat(e.getMessage(), containsString("Reconnection abandoned"));
    }
    verify(connectionSupplier, atLeast(2)).get();
    assertThat(observedConnection.get(), is(nullValue()));

    int connectionSupplierGetCount = (int)Mockito.mockingDetails(connectionSupplier).getInvocations().stream()
        .filter(i -> i.getMethod().getName().equals("get")).count();

    /*
     * Try another reconnect; this one is expected to complete within the timeout.
     */
    actionQueue.set(getConnectionActions);
    getConnectionActions.addAll(Collections.singletonList(GetConnectionAction.RAISE));

    reconnectableConnection.reconnect();
    verify(connectionSupplier, times(connectionSupplierGetCount + 2)).get();
    assertThat(observedConnection.get(), is(notNullValue()));
  }

  /**
   * Test the functionality of a {@link ReconnectableLeasedConnection} in conjunction with a fetched
   * {@link EntityRef}.  This method does way too much but follows a natural progression of a connection
   * lifecycle.
   */
  private <R extends EntityRef<?, ?, ?>, E extends Entity>
  void testEntityRef(Class<R> entityRefClass, Class<E> entityClass, long entityVersion, String entityName)
      throws Exception {
    StringWriter sw = new StringWriter(4096);
    Logger connectionLogger = LogManager.getLogger(ReconnectableLeasedConnection.class);
    Appender connectionAppender = new WriterAppender(new PatternLayout("%p %m%n"), sw);
    connectionLogger.addAppender(connectionAppender);
    connectionLogger.setLevel(Level.INFO);

    try {
      ConnectionSupplier connectionSupplier = mock(ConnectionSupplier.class);

      AtomicReference<LeasedConnection> observedConnection = new AtomicReference<>();
      Queue<GetConnectionAction> getConnectionActions = new ConcurrentLinkedQueue<>();
      List<EntityRef<E, Object, Object>> deliveredEntityRefs = new ArrayList<>();
      when(connectionSupplier.get()).then(
          (Answer<LeasedConnection>)invocationOnMock -> getLeasedConnection(observedConnection, deliveredEntityRefs, getConnectionActions));

      ReconnectableLeasedConnection reconnectableConnection = new ReconnectableLeasedConnection(connectionSupplier, 0L);
      LeasedConnection firstConnection = observedConnection.get();

      EntityRef<E, Object, Object> fetchedEntityRef =
          reconnectableConnection.getEntityRef(entityClass, entityVersion, entityName);

      assertThat(deliveredEntityRefs, hasSize(1));
      assertThat(classCaptor.getValue(), is(equalTo(entityClass)));
      assertThat(longCaptor.getValue(), is(entityVersion));
      assertThat(stringCaptor.getValue(), is(entityName));

      assertThat(fetchedEntityRef, isA(EntityRef.class));
      assertThat(fetchedEntityRef, is(instanceOf(entityRefClass)));

      /*
       * Initiate a reconnect ...
       */
      reconnectableConnection.reconnect();
      LeasedConnection secondConnection = observedConnection.get();

      assertThat(secondConnection, is(not(sameInstance(firstConnection))));
      assertThat(deliveredEntityRefs, hasSize(2));

      /*
       * As a stand-in for ensuring ReconnectableEntityRef.swap is called by reconnect() to replace the
       * delegate EntityRef, we'll call fetchedEntityRef.getName() which should be passed along only to
       * the current delegate.
       */
      fetchedEntityRef.getName();   // Don't care about the returned value
      verify(deliveredEntityRefs.get(0), never()).getName();
      verify(deliveredEntityRefs.get(1)).getName();

      /*
       * Initiate another reconnect with the thread interrupted.
       */
      Thread.currentThread().interrupt();
      try {
        reconnectableConnection.reconnect();
        fail("Expecting InterruptedException");
      } catch (InterruptedException e) {
        assertThat(e.getMessage(), containsString("during reconnection"));
      }
      assertFalse(Thread.currentThread().isInterrupted());
      assertThat(deliveredEntityRefs, hasSize(2));

      /*
       * Initiate another reconnect with a ConnectionClosedException thrown into the mix.
       */
      getConnectionActions.add(GetConnectionAction.RAISE);
      reconnectableConnection.reconnect();
      assertTrue(getConnectionActions.isEmpty());
      LeasedConnection thirdConnection = observedConnection.get();

      assertThat(thirdConnection, is(not(sameInstance(secondConnection))));
      assertThat(deliveredEntityRefs, hasSize(3));

      fetchedEntityRef.getName();
      verify(deliveredEntityRefs.get(2)).getName();

      /*
       * Initiate yet another reconnect with a ConnectionClosedException WITH an interrupt.
       */
      sw.getBuffer().setLength(0);    // Empty the log buffer
      getConnectionActions.add(GetConnectionAction.RAISE);                // [0]
      getConnectionActions.add(GetConnectionAction.RAISE);                // [1]
      getConnectionActions.add(GetConnectionAction.INTERRUPT_AND_RAISE);  // [2]
      try {
        reconnectableConnection.reconnect();
        fail("Expecting InterruptedException");
      } catch (InterruptedException e) {
        // expected
      }
      assertTrue(getConnectionActions.isEmpty());
      assertFalse(Thread.currentThread().isInterrupted());
      assertThat(deliveredEntityRefs, hasSize(3));

      String lastLogLine = Arrays.stream(sw.getBuffer().toString().split("\\R"))
          .reduce((prev, next) -> next)
          .orElseThrow(() -> new AssertionError("Unexpected logging output: " + sw.getBuffer()));
      assertThat(lastLogLine, stringContainsInOrder(Arrays.asList("Reconnect failed", "[2]", "ConnectionClosedException")));

      /*
       * Close ...
       */
      reconnectableConnection.close();
      verify(firstConnection, never()).close();
      verify(secondConnection, never()).close();
      verify(thirdConnection).close();

      /*
       * After explicit close, reconnect should fail
       */
      try {
        reconnectableConnection.reconnect();
        fail("Expecting ConnectionClosedException");
      } catch (ConnectionClosedException e) {
        assertThat(e.getMessage(), containsString("explicitly closed"));
      }

    } finally {
      connectionLogger.removeAppender(connectionAppender);
    }
  }

  private enum GetConnectionAction {
    RAISE,
    INTERRUPT_AND_RAISE
  }

  private <E extends Entity>
  LeasedConnection getLeasedConnection(AtomicReference<LeasedConnection> observedConnection,
                                       List<EntityRef<E, Object, Object>> deliveredEntityRefs,
                                       Queue<GetConnectionAction> preConnectionActions)
      throws org.terracotta.exception.EntityNotProvidedException {

    GetConnectionAction action = preConnectionActions.poll();
    if (action != null) {
      switch (action) {
        case INTERRUPT_AND_RAISE:
          Thread.currentThread().interrupt();
          throw new ConnectionClosedException("Connection closed");
        case RAISE:
          throw new ConnectionClosedException("Connection closed");
      }
    }

    LeasedConnection connection = ReconnectableLeasedConnectionTest.this.getConnection(observedConnection::set);

    when(observedConnection.get().getEntityRef(classCaptor.capture(), longCaptor.capture(), stringCaptor.capture()))
            .thenAnswer((Answer<EntityRef<E, Object, Object>>) iom -> {
          @SuppressWarnings("unchecked") EntityRef<E, Object, Object> mockEntityRef = mock(EntityRef.class);
          deliveredEntityRefs.add(mockEntityRef);
          return mockEntityRef;
        });
    return connection;
  }

  private static class RepeatingQueue<E> implements Queue<E> {
    private final AtomicReference<E> element = new AtomicReference<>();

    private RepeatingQueue(E element) {
      this.element.set(element);
    }

    public void setElement(E element) {
      this.element.set(element);
    }

    @Override
    public int size() {
      return (element.get() == null ? 0 : 1);
    }

    @Override
    public boolean isEmpty() {
      return element.get() == null;
    }

    @Override
    public boolean contains(Object o) {
      E e = element.get();
      return e != null && e.equals(o);
    }

    @Override
    public Iterator<E> iterator() {
      E e = element.get();
      return (e == null ? Collections.<E>emptySet() : Collections.singleton(e)).iterator();
    }

    @Override
    public Object[] toArray() {
      E e = element.get();
      return (e == null ? new Object[0] : new Object[] { e });
    }

    @Override
    public <T> T[] toArray(T[] a) {
      E e = element.get();
      return (e == null ? Collections.<E>emptySet() : Collections.singleton(e)).toArray(a);
    }

    @Override
    public boolean add(E e) {
      E e1 = element.get();
      if (e1 != null && e1.equals(e)) {
        return false;
      } else {
        throw new IllegalStateException();
      }
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      E e = element.get();
      return e != null && c.stream().allMatch(i -> Objects.equals(i, e));
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      element.set(null);
    }

    @Override
    public boolean offer(E e) {
      return false;
    }

    @Override
    public E remove() {
      E e = element.get();
      if (e == null) {
        throw new NoSuchElementException();
      }
      return e;
    }

    @Override
    public E poll() {
      return element.get();
    }

    @Override
    public E element() {
      E e = element.get();
      if (e == null) {
        throw new NoSuchElementException();
      }
      return e;
    }

    @Override
    public E peek() {
      return element.get();
    }
  }

  private LeasedConnection getConnection(Consumer<LeasedConnection> observer) {
    LeasedConnection leasedConnection = mock(LeasedConnection.class);
    observer.accept(leasedConnection);
    return leasedConnection;
  }
}