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
package com.terracottatech.testing.delay;

import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.exception.EntityException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DelayInvokeFuture<T extends EntityResponse> implements InvokeFuture<T> {
  private final Delay delay;
  private final InvokeFuture<T> underlying;

  public DelayInvokeFuture(Delay delay, InvokeFuture<T> underlying) {
    this.delay = delay;
    this.underlying = underlying;
  }

  @Override
  public boolean isDone() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T get() throws InterruptedException, EntityException {
    T result = underlying.get();
    delay.serverToClientDelay(result);
    return result;
  }

  @Override
  public T getWithTimeout(long unitCount, TimeUnit timeUnit) throws InterruptedException, EntityException, TimeoutException {
    T result = underlying.getWithTimeout(unitCount, timeUnit);
    delay.serverToClientDelay(result);
    return result;
  }

  @Override
  public void interrupt() {
    underlying.interrupt();
  }
}
