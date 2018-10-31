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
package com.terracottatech.store.client;

import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.exception.EntityException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class InvokeFutureFuture<V extends EntityResponse> implements Future<V> {
  private final InvokeFuture<V> invokeFuture;

  public InvokeFutureFuture(InvokeFuture<V> invokeFuture) {
    this.invokeFuture = invokeFuture;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (mayInterruptIfRunning) {
      invokeFuture.interrupt();
    }

    return mayInterruptIfRunning;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return invokeFuture.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    try {
      return invokeFuture.get();
    } catch (EntityException e) {
      throw new ExecutionException(e);
    }
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    try {
      return invokeFuture.getWithTimeout(timeout, unit);
    } catch (EntityException e) {
      throw new ExecutionException(e);
    }
  }
}
