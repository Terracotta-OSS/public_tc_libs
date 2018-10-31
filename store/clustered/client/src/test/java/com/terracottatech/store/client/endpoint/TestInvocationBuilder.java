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
package com.terracottatech.store.client.endpoint;

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import org.terracotta.entity.InvocationBuilder;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.InvokeMonitor;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class TestInvocationBuilder implements InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> {
  private final Responder responder;
  private DatasetEntityMessage message;

  public TestInvocationBuilder(Responder responder) {
    this.responder = responder;
  }

  @Override
  public InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> ackSent() {
    return this;
  }

  @Override
  public InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> ackReceived() {
    return this;
  }

  @Override
  public InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> ackCompleted() {
    return this;
  }

  @Override
  public InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> ackRetired() {
    return this;
  }

  @Override
  public InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> replicate(boolean b) {
    return this;
  }

  @Override
  public InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> message(DatasetEntityMessage message) {
    this.message = message;
    return this;
  }

  @Override
  public InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> monitor(InvokeMonitor<DatasetEntityResponse> invokeMonitor) {
    return null;
  }

  @Override
  public InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> withExecutor(Executor executor) {
    return null;
  }

  @Override
  public InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> asDeferredResponse() {
    return null;
  }

  @Override
  public InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> blockGetOnRetire(boolean b) {
    return this;
  }

  @Override
  public InvokeFuture<DatasetEntityResponse> invoke() {
    DatasetEntityResponse response = responder.respond(message);
    return new TestInvokeFuture(response);
  }

  @Override
  public InvokeFuture<DatasetEntityResponse> invokeWithTimeout(long l, TimeUnit timeUnit) {
    throw new UnsupportedOperationException("Not needed by current test - implement if required");
  }
}
