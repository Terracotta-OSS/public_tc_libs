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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.entity.Entity;

import com.terracottatech.store.StoreRuntimeException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A non-specialized {@link Entity} delegating operations to a <i>replaceable</i> instance.
 */
class ReconnectableProxyEntity<T extends Entity>
    extends AbstractReconnectableEntity<T>
    implements InvocationHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectableProxyEntity.class);

  private final AtomicReference<T> delegate = new AtomicReference<>();

  private ReconnectableProxyEntity(T delegate, ReconnectController reconnectController) {
    super(reconnectController);
    this.delegate.set(delegate);
  }

  /**
   * Creates a {@link Proxy} for an {@link Entity} to provide reconnection capabilities to the {@code Entity}.
   * @param delegate the {@code Entity} for which the proxy is created
   * @param reconnectController the {@code ReconnectController} to manage reconnection
   * @param <T> the type of {@code delegate}
   * @param <R> the reconnectable subtype of {@code <T>}
   * @return a new {@code Entity} proxy for {@code delegate}
   */
  @SuppressWarnings("unchecked")
  public static <T extends Entity, R extends ReconnectableEntity<T>> R getProxy(T delegate, ReconnectController reconnectController) {
    Class<? extends Entity> delegateClass = delegate.getClass();

    /*
     * Ensure delegate does not override equals and hashCode ... since the delegate can be
     * replaced, equals and hashCode must be supplied by this class (for value stability)
     * and, if the delegate implements equals and hashCode, expectations of their use will
     * likely not be met.
     */
    try {
      if (!delegateClass.getMethod("equals", Object.class).getDeclaringClass().equals(Object.class)) {
        throw new AssertionError("Entity type '" + delegateClass.getName() + "' implements equals(Object)");
      }
      if (!delegateClass.getMethod("hashCode").getDeclaringClass().equals(Object.class)) {
        throw new AssertionError("Entity type '" + delegateClass.getName() + "' implements hashCode()");
      }
    } catch (NoSuchMethodException e) {
      throw new AssertionError("Unable to create proxy for " + delegate, e);
    }

    ReconnectableProxyEntity<T> proxyEntity = new ReconnectableProxyEntity<>(delegate, reconnectController);
    Class<?>[] interfaces = delegateClass.getInterfaces();
    Class<?>[] aggregateInterfaces = new Class<?>[interfaces.length + 1];
    aggregateInterfaces[0] = ReconnectableEntity.class;   // First to establish "ownership" for overridden methods
    System.arraycopy(interfaces, 0, aggregateInterfaces, 1, interfaces.length);
    R reconnectableEntity;
    try {
      reconnectableEntity = (R)Proxy.newProxyInstance(delegateClass.getClassLoader(), aggregateInterfaces, proxyEntity);    // unchecked
    } catch (RuntimeException e) {
      LOGGER.error("Unable to create proxy for {}", delegate, e);
      throw new AssertionError("Unable to create proxy for " + delegate, e);
    }
    return reconnectableEntity;
  }

  @Override
  protected T getDelegate() {
    return delegate.get();
  }

  @Override
  public void swap(T newEntity) {
    delegate.set(newEntity);
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  /**
   * Identifies the classes whose method invocations are passed directly to the ReconnectableProxyEntity
   * instance and not passed to the Entity delegate.  Methods declared in ReconnectableEntity MUST be
   * implemented in AbstractReconnectableEntity or, by extension, **this** class.
   */
  private static final Set<Class<?>> NON_PROXY_CLASSES = Collections.newSetFromMap(new IdentityHashMap<>());
  static {
    NON_PROXY_CLASSES.addAll(Arrays.asList(Object.class, ReconnectableEntity.class));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

    /*
     * Directly invoke "internal" methods.
     */
    if (NON_PROXY_CLASSES.contains(method.getDeclaringClass())) {
      if (method.getExceptionTypes().length == 0) {
        return invokeWrappingException(method, this, args);
      } else {
        return invokeUnwrappingException(method, this, args);
      }
    }

    /*
     * Invoke Entity methods through the ReconnectController.
     */
    if (method.getExceptionTypes().length == 0) {
      return reconnectController.withConnectionClosedHandling(
          () -> invokeWrappingException(method, getDelegate(), args));
    } else {
      return reconnectController.withConnectionClosedHandlingExceptionally(
          () -> invokeUnwrappingException(method, getDelegate(), args));
    }
  }

  @SuppressWarnings("Duplicates")
  private <I> Object invokeWrappingException(Method method, I instance, Object[] args) {
    try {
      return method.invoke(instance, args);
    } catch (IllegalAccessException | IllegalArgumentException e) {
      LOGGER.error("Error invoking {} via proxy for {}", method, instance, e);
      throw new AssertionError(e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException)cause;
      } else if (cause instanceof Error) {
        throw (Error)cause;
      } else {
        throw new StoreRuntimeException(e);
      }
    }
  }

  private <I>  Object invokeUnwrappingException(Method method, I instance, Object[] args) throws Exception {
    try {
      return method.invoke(instance, args);
    } catch (IllegalAccessException | IllegalArgumentException e) {
      LOGGER.error("Error invoking {} via proxy for {}", method, instance, e);
      throw new AssertionError(e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException)cause;
      } else if (cause instanceof Error) {
        throw (Error)cause;
      } else if (cause instanceof Exception) {
        throw (Exception)cause;
      } else {
        throw new StoreRuntimeException(cause);
      }
    }
  }
}
