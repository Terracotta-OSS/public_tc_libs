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
package com.terracottatech.store.common.messages.stream.inline;

import com.terracottatech.store.Record;
import com.terracottatech.store.intrinsics.IntrinsicType;
import com.terracottatech.store.intrinsics.impl.LeafIntrinsicPredicate;

import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * An internal "marker" {@link Predicate} used by a clustered client to signal an server-to-client
 * response point in a {@link java.util.stream.Stream Stream&lt;Record&lt;K>>}.  An instance of
 * this predicate is provided to the server as a {@link  Stream#filter(Predicate) filter} operation
 * parameter in a portable pipeline sequence.
 */
public class WaypointMarker<K extends Comparable<K>> extends LeafIntrinsicPredicate<Record<K>> {
  /**
   * Indicates the client-side operation to which this waypoint is bound.
   */
  private final int waypointId;

  /**
   * Creates a new waypoint marker.
   * @param waypointId the identifier of the client-side operation to which this waypoint is bound
   */
  public WaypointMarker(int waypointId) {
    super(IntrinsicType.WAYPOINT);
    this.waypointId = waypointId;
  }

  /**
   * Gets the id of the stream operation to which this waypoint is bound.
   * @return the operation id
   */
  public int getWaypointId() {
    return waypointId;
  }

  @Override
  public boolean test(Record<K> cells) {
    throw new AssertionError(this.getClass().getSimpleName() + " is not an operational Predicate");
  }

  @Override
  public String toString() {
    return "WaypointMarker{waypointId=" + waypointId + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WaypointMarker<?> that = (WaypointMarker<?>) o;
    return waypointId == that.waypointId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(waypointId);
  }
}
