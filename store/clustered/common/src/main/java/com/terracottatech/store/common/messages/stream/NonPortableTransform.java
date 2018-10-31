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
package com.terracottatech.store.common.messages.stream;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.intrinsics.IntrinsicType;
import com.terracottatech.store.intrinsics.impl.LeafIntrinsicUpdateOperation;

/**
 * A flag {@link UpdateOperation} indicating a non-portable transform.
 */
public class NonPortableTransform<K extends Comparable<K>> extends LeafIntrinsicUpdateOperation<K> {

  private final int waypointId;

  public NonPortableTransform(int waypointId) {
    super(IntrinsicType.NON_PORTABLE_TRANSFORM);
    this.waypointId = waypointId;
  }

  public int getWaypointId() {
    return waypointId;
  }

  @Override
  public Iterable<Cell<?>> apply(Record<K> t) {
    throw new AssertionError(this.getClass().getSimpleName() + " is not an operational transform");
  }

  @Override
  public String toString() {
    return "NonPortableTransform{waypointId=" + waypointId + '}';
  }
}
