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
import org.terracotta.entity.EntityResponse;

public class DelayEndpointDelegate<R extends EntityResponse> implements EndpointDelegate<R> {
  private final Delay delay;
  private final EndpointDelegate<R> underlying;

  public DelayEndpointDelegate(Delay delay, EndpointDelegate<R> underlying) {
    this.delay = delay;
    this.underlying = underlying;
  }

  @Override
  public void handleMessage(R entityResponse) {
    delay.serverToClientDelay(entityResponse);
    underlying.handleMessage(entityResponse);
  }

  @Override
  public byte[] createExtendedReconnectData() {
    return underlying.createExtendedReconnectData();
  }

  @Override
  public void didDisconnectUnexpectedly() {
    underlying.didDisconnectUnexpectedly();
  }
}
