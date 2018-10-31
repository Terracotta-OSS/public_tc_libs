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
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.terracotta.connection.entity.Entity;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests functionality of {@link ReconnectableProxyEntity}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ReconnectableProxyEntityTest {

  @Mock
  private ReconnectController reconnectController;

  @Before
  public void setUp() {
    when(reconnectController.withConnectionClosedHandling(any()))
        .thenAnswer(invocation -> {
          Supplier<Object> supplier = invocation.getArgument(0);
          return supplier.get();
        });
  }

  @Test
  public void testGetProxy() {
    TestEntity delegate = new TestEntityImpl();
    ReconnectableEntity<TestEntity> proxy = ReconnectableProxyEntity.getProxy(delegate, reconnectController);
    assertNotNull(proxy);
    assertThat(proxy, is(instanceOf(TestEntity.class)));
    assertThat(Proxy.getInvocationHandler(proxy), is(instanceOf(ReconnectableProxyEntity.class)));
    assertThat(((ReconnectableProxyEntity)Proxy.getInvocationHandler(proxy)).getDelegate(), is(delegate));
  }

  @Test
  public void testGetBadProxyEquals() {
    TestEntity delegate = new BadTestEntityEqualsImpl();
    try {
      @SuppressWarnings("unused") ReconnectableEntity<TestEntity> proxy = ReconnectableProxyEntity.getProxy(delegate, reconnectController);
      fail("Expecting AssertionError");
    } catch (AssertionError e) {
      assertThat(e.getMessage(), containsString("equals"));
    }
  }

  @Test
  public void testGetBadProxyHashCode() {
    TestEntity delegate = new BadTestEntityHashCodeImpl();
    try {
      @SuppressWarnings("unused") ReconnectableEntity<TestEntity> proxy = ReconnectableProxyEntity.getProxy(delegate, reconnectController);
      fail("Expecting AssertionError");
    } catch (AssertionError e) {
      assertThat(e.getMessage(), containsString("hashCode"));
    }
  }

  @Test
  public void testToStringProxy() {
    TestEntity delegate = new TestEntityImpl();
    ReconnectableEntity<TestEntity> proxy = ReconnectableProxyEntity.getProxy(delegate, reconnectController);
    assertThat(proxy.toString(), is("TestEntityImpl{}"));
  }

  @Test
  public void testMethodProxy() {
    TestEntity delegate = new TestEntityImpl();
    ReconnectableEntity<TestEntity> proxy = ReconnectableProxyEntity.getProxy(delegate, reconnectController);
    Object arg = new Object();
    ((TestEntity)proxy).method(arg);
    assertThat(((TestEntityImpl)delegate).methodCapture.get(), is(sameInstance(arg)));
  }

  @Test
  public void testClose() {
    TestEntity delegate = new TestEntityImpl();
    ReconnectableEntity<TestEntity> proxy = ReconnectableProxyEntity.getProxy(delegate, reconnectController);

    proxy.close();

    assertTrue(((TestEntityImpl)delegate).closed.get());
  }

  @Test
  public void testOnClose() {
    TestEntity delegate = new TestEntityImpl();
    ReconnectableEntity<TestEntity> proxy = ReconnectableProxyEntity.getProxy(delegate, reconnectController);

    AtomicBoolean closeObserved = new AtomicBoolean();
    proxy.onClose(() -> closeObserved.set(true));

    proxy.close();

    assertTrue(closeObserved.get());

    try {
      proxy.onClose(() -> {});
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testSwap() {
    TestEntity delegate = new TestEntityImpl();
    ReconnectableEntity<TestEntity> proxy = ReconnectableProxyEntity.getProxy(delegate, reconnectController);

    @SuppressWarnings("unchecked")
    ReconnectableProxyEntity<TestEntity> invocationHandler = (ReconnectableProxyEntity<TestEntity>)Proxy.getInvocationHandler(proxy);

    TestEntity secondDelegate = new TestEntityImpl();
    invocationHandler.swap(secondDelegate);

    assertThat(invocationHandler.getDelegate(), is(sameInstance(secondDelegate)));
  }

  private interface TestEntity extends Entity {
    void method(Object arg);
  }

  private static class TestEntityImpl implements TestEntity {

    private AtomicBoolean closed = new AtomicBoolean();
    private AtomicReference<Object> methodCapture = new AtomicReference<>();

    @Override
    public void method(Object arg) {
      methodCapture.set(arg);
    }

    @Override
    public void close() {
      closed.set(true);
    }

    @Override
    public String toString() {
      return "TestEntityImpl{}";
    }
  }

  private static class BadTestEntityHashCodeImpl implements TestEntity {

    @Override
    public void method(Object arg) {
    }

    @Override
    public void close() {
    }

    @Override
    public int hashCode() {
      return 1;
    }
  }

  @SuppressWarnings("overrides")
  private static class BadTestEntityEqualsImpl implements TestEntity {

    @Override
    public void method(Object arg) {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean equals(Object obj) {
      return false;
    }
  }
}