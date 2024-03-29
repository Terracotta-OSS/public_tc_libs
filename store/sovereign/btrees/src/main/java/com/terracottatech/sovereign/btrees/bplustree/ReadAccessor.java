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
package com.terracottatech.sovereign.btrees.bplustree;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class ReadAccessor implements Comparable<ReadAccessor> {
  private static AtomicLong genId = new AtomicLong();
  private final Object obj;
  private final long revision;
  private int stripe;
  private long id = genId.incrementAndGet();

  public ReadAccessor(Object obj, long revision) {
    this.obj = obj;
    this.revision = revision;
    this.stripe = -1;
  }

  @Override
  public String toString() {
    return "SlotAccessor{" + "obj=" + obj + ", revision=" + revision + ", stripe=" + stripe + '}';
  }

  public int getStripe() {
    return stripe;
  }

  public long getRevision() {
    return revision;
  }

  public Object getObject() {
    return obj;
  }

  @Override
  public int compareTo(ReadAccessor o) {
    int ret = Long.compare(revision, o.revision);
    if (ret == 0) {
      ret = Long.compare(id, o.id);
    }
    return ret;
  }

  public ReadAccessor stripe(int s) {
    this.stripe = s;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReadAccessor accessor = (ReadAccessor) o;
    return id == accessor.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
