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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

public class DestroyableReferenceCount {
  private static final int DESTROYED = -1;

  private final AtomicInteger referenceCount = new AtomicInteger(0);

  /**
   * @return true if the reference count was incremented, false if the reference has been marked as destroyed
   */
  public boolean increment() {
    int currentClientCount = referenceCount.updateAndGet(new IncrementIfNotDestroyed());
    assert(currentClientCount != 0);
    return currentClientCount > 0;
  }

  public void decrement() {
    referenceCount.updateAndGet(new DecrementIfNotDestroyed());
  }

  public DestructionResult destroy() {
    int previousClientCount = referenceCount.getAndUpdate(new DestroyIfNoClients());

    if (previousClientCount > 0) {
      return new DestructionResult(false, previousClientCount, false);
    } else {
      boolean fresh = previousClientCount == 0;
      return new DestructionResult(true, 0, fresh);
    }
  }

  private static class IncrementIfNotDestroyed implements IntUnaryOperator {
    @Override
    public int applyAsInt(int current) {
      if (current == DESTROYED) {
        return DESTROYED;
      }

      return current + 1;
    }
  }

  private static class DecrementIfNotDestroyed implements IntUnaryOperator {
    @Override
    public int applyAsInt(int current) {
      assert (current != 0);

      if (current == DESTROYED) {
        return DESTROYED;
      }

      return current - 1;
    }
  }

  private static class DestroyIfNoClients implements IntUnaryOperator {
    @Override
    public int applyAsInt(int current) {
      if (current == 0) {
        return DESTROYED;
      }

      return current;
    }
  }
}
