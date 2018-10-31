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

import org.terracotta.connection.entity.Entity;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The base for a reconnectable {@link Entity} instance.
 */
abstract class AbstractReconnectableEntity<T extends Entity> implements ReconnectableEntity<T> {
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicReference<Runnable> closeAction = new AtomicReference<>(null);

  protected final ReconnectController reconnectController;

  public AbstractReconnectableEntity(ReconnectController reconnectController) {
    this.reconnectController = reconnectController;
  }

  protected abstract T getDelegate();

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      try {
        reconnectController.withConnectionClosedHandling(() -> {
          getDelegate().close();
          return null;
        });
      } finally {
        Optional.ofNullable(closeAction.get()).ifPresent(Runnable::run);
      }
    }
  }

  /**
   * Registers an action to take place when this {@code DatasetEntity} is closed.
   * @param closeAction the action to be performed
   */
  @Override
  public void onClose(Runnable closeAction) {
    if (closed.get()) {
      throw new IllegalStateException(this.getClass().getName() + " already closed");
    }
    this.closeAction.accumulateAndGet(closeAction, AbstractReconnectableEntity::chain);
  }

  @SuppressWarnings("Duplicates")
  private static Runnable chain(Runnable first, Runnable second) {
    if (first == null) {
      return second;
    } else if (second == null) {
      return first;
    }
    return () -> {
      try {
        first.run();
      } catch (Throwable t1) {
        try {
          second.run();
        } catch (Throwable t2) {
          try {
            // addSuppressed is not supported for some Throwables
            t1.addSuppressed(t2);
          } catch (Exception ignored) {
          }
        }
        throw t1;
      }
      second.run();
    };
  }
}
