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
package com.terracottatech.store.client.message;

import com.terracottatech.store.common.messages.DatasetEntityResponse;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ResponseProcessingFuture<R extends DatasetEntityResponse> implements Future<R> {
  private final Future<DatasetEntityResponse> underlying;
  private final ResponseProcessor processor;

  public ResponseProcessingFuture(Future<DatasetEntityResponse> underlying, ResponseProcessor processor) {
    this.underlying = underlying;
    this.processor = processor;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return underlying.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return underlying.isCancelled();
  }

  @Override
  public boolean isDone() {
    return underlying.isDone();
  }

  @Override
  public R get() throws InterruptedException, ExecutionException {
    DatasetEntityResponse response = underlying.get();
    return processResponse(response);
  }

  @Override
  public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    DatasetEntityResponse response = underlying.get(timeout, unit);
    return processResponse(response);
  }

  private R processResponse(DatasetEntityResponse response) throws ExecutionException {
    try {
      return processor.process(response);
    } catch (RuntimeException e) {
      throw new ExecutionException(e);
    }
  }
}