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

package com.terracottatech.store.intrinsics.impl;

import com.terracottatech.store.internal.InternalRecord;

import java.util.Objects;

import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_RECORD_SAME;

public class RecordSameness<K extends Comparable<K>> extends LeafIntrinsicPredicate<InternalRecord<K>> {

  private final long msn;
  private final K key;

  public RecordSameness(long msn, K key) {
    super(PREDICATE_RECORD_SAME);
    this.msn = msn;
    this.key = key;
  }

  public long getMSN() {
    return msn;
  }

  public K getKey() {
    return key;
  }

  @Override
  public boolean test(InternalRecord<K> r) {
    return msn == r.getMSN() && key.equals(r.getKey());
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RecordSameness<?> that = (RecordSameness<?>) o;
    return msn == that.msn &&
            Objects.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(msn, key);
  }
}
