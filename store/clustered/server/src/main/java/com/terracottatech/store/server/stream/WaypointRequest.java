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
package com.terracottatech.store.server.stream;

import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchWaypointResponse;
import com.terracottatech.store.common.messages.stream.inline.WaypointMarker;

import java.util.concurrent.CompletableFuture;

/**
 * A {@link NonElement} completion object through which a waypoint in a mutative pipeline
 * requests continuation of the client-side pipeline into the mutation transformation
 * computation.
 *
 * @see InlineElementSource#continueFetch(int, Object)
 * @see WaypointMarker
 * @see TryAdvanceFetchWaypointResponse
 */
public final class WaypointRequest<E> extends NonElement {
  private final int waypointId;
  private final E pendingElement;
  private final CompletableFuture<Object> responseFuture = new CompletableFuture<>();

  public WaypointRequest(int waypointId, E pendingElement) {
    this.waypointId = waypointId;
    this.pendingElement = pendingElement;
  }

  public int getWaypointId() {
    return this.waypointId;
  }

  public E getPendingElement() {
    return this.pendingElement;
  }

  public CompletableFuture<Object> getResponseFuture() {
    return this.responseFuture;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WaypointRequest{");
    sb.append("waypointId=").append(waypointId);
    sb.append(", pendingElement=").append(pendingElement);
    sb.append(", responseFuture=").append(responseFuture);
    sb.append('}');
    return sb.toString();
  }
}
