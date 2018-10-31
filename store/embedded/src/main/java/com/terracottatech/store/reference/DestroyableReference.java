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
package com.terracottatech.store.reference;

import com.terracottatech.store.StoreException;
import com.terracottatech.store.common.InterruptHelper;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class DestroyableReference<T> {
  private final DestroyableReferenceCount referenceCount = new DestroyableReferenceCount();
  private final CompletableFuture<T> futureValue = new CompletableFuture<>();

  public void setValue(T value) {
    if (!this.futureValue.complete(value)) {
      throw new IllegalStateException("Either set the value or provide an exception - only call once");
    }
  }

  public void exception(Throwable t) {
    if (!futureValue.completeExceptionally(t)) {
      throw new IllegalStateException("Either set the value or provide an exception - only call once");
    }
  }

  public T acquireValue() {
    T value = null;
    try {
      boolean incremented = referenceCount.increment();

      if (!incremented) {
        return null;
      }

      value = getUninterruptibly();

      return value;
    } finally {
      if (value == null) {
        referenceCount.decrement();
      }
    }
  }

  private T getUninterruptibly() {

    try {
      return InterruptHelper.getUninterruptibly(futureValue);
    } catch (ExecutionException e) {
      return null;
    }
  }

  public void releaseValue() {
    referenceCount.decrement();
  }

  public Optional<T> destroy() throws StoreException {
    if (!futureValue.isDone()) {
      throw new StoreException("Cannot destroy, reference count: 1");
    }

    if (futureValue.isCompletedExceptionally()) {
      return Optional.empty();
    }

    DestructionResult destructionResult = referenceCount.destroy();

    if (!destructionResult.isDestroyed()) {
      throw new StoreException("Cannot destroy, reference count: " + destructionResult.getCount());
    }

    if (!destructionResult.isFresh()) {
      return Optional.empty();
    }

    T value = getAvailableValue();
    return Optional.of(value);
  }

  public boolean isAvailable() {
    return futureValue.isDone() && !futureValue.isCompletedExceptionally();
  }

  /**
   * Only call this if isAvailable() returns true.
   * Note that it does not increment the reference count
   *
   * @return the available value
   */
  public T getAvailableValue() {
    try {
      return futureValue.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new AssertionError();
    }
  }
}
