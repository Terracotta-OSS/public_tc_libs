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

import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvocationBuilder;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.InvokeMonitor;
import org.terracotta.entity.MessageCodecException;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DelayInvocationBuilder<M extends EntityMessage, R extends EntityResponse> implements InvocationBuilder<M, R> {
  private final Delay delay;
  private final InvocationBuilder<M, R> underlying;
  private volatile EntityMessage message;

  public DelayInvocationBuilder(Delay delay, InvocationBuilder<M, R> underlying) {
    this.delay = delay;
    this.underlying = underlying;
    this.message = null;
  }

  private DelayInvocationBuilder(Delay delay, InvocationBuilder<M, R> underlying, EntityMessage message) {
    this.delay = delay;
    this.underlying = underlying;
    this.message = message;
  }

  @Override
  public InvocationBuilder<M, R> ackSent() {
    return new DelayInvocationBuilder<>(delay, underlying.ackSent(), message);
  }

  @Override
  public InvocationBuilder<M, R> ackReceived() {
    return new DelayInvocationBuilder<>(delay, underlying.ackReceived(), message);
  }

  @Override
  public InvocationBuilder<M, R> ackCompleted() {
    return new DelayInvocationBuilder<>(delay, underlying.ackCompleted(), message);
  }

  @Override
  public InvocationBuilder<M, R> ackRetired() {
    return new DelayInvocationBuilder<>(delay, underlying.ackRetired(), message);
  }

  @Override
  public InvocationBuilder<M, R> replicate(boolean b) {
    return new DelayInvocationBuilder<>(delay, underlying.replicate(b), message);
  }

  @Override
  public InvocationBuilder<M, R> message(M m) {
    message = m;
    return new DelayInvocationBuilder<>(delay, underlying.message(m), m);
  }

  @Override
  public InvocationBuilder<M, R> monitor(InvokeMonitor<R> invokeMonitor) {
    return null;
  }

  @Override
  public InvocationBuilder<M, R> withExecutor(Executor executor) {
    return null;
  }

  @Override
  public InvocationBuilder<M, R> asDeferredResponse() {
    return null;
  }

  @Override
  public InvocationBuilder<M, R> blockGetOnRetire(boolean b) {
    return new DelayInvocationBuilder<>(delay, underlying.blockGetOnRetire(b), message);
  }

  @Override
  public InvokeFuture<R> invoke() throws MessageCodecException {
    delay.clientToServerDelay(message);
    InvokeFuture<R> invokeFuture = underlying.invoke();
    return new DelayInvokeFuture<>(delay, invokeFuture);
  }

  @Override
  public InvokeFuture<R> invokeWithTimeout(long l, TimeUnit timeUnit) throws InterruptedException, TimeoutException, MessageCodecException {
    delay.clientToServerDelay(message);
    InvokeFuture<R> invokeFuture = underlying.invokeWithTimeout(l, timeUnit);
    return new DelayInvokeFuture<>(delay, invokeFuture);
  }
}
