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

import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvocationBuilder;

import java.util.concurrent.Future;

public class DelayEntityClientEndpoint<M extends EntityMessage, R extends EntityResponse> implements EntityClientEndpoint<M, R> {
  private final EntityClientEndpoint<M, R> underlying;
  private final Delay delay;

  public DelayEntityClientEndpoint(EntityClientEndpoint<M, R> underlying, Delay delay) {
    this.underlying = underlying;
    this.delay = delay;
  }

  @Override
  public byte[] getEntityConfiguration() {
    return underlying.getEntityConfiguration();
  }

  @Override
  public void setDelegate(EndpointDelegate<R> endpointDelegate) {
    EndpointDelegate<R> delayEndpointDelegate = new DelayEndpointDelegate<>(delay, endpointDelegate);
    underlying.setDelegate(delayEndpointDelegate);
  }

  @Override
  public InvocationBuilder<M, R> beginInvoke() {
    InvocationBuilder<M, R> builder = underlying.beginInvoke();
    return new DelayInvocationBuilder<>(delay, builder);
  }

  @Override
  public void close() {
    underlying.close();
  }

  @Override
  public Future<Void> release() {
    return underlying.release();
  }
}
