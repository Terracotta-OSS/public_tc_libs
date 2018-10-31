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

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class SingleListBasedAccessTrackerImpl implements ListBasedAccessTracker {
  private ConcurrentSkipListMap<ReadAccessor, Object> active = new ConcurrentSkipListMap<>();

  public SingleListBasedAccessTrackerImpl() {

  }

  @Override
  public void deregisterRead(ReadAccessor accessor) {
    active.remove(accessor);
  }

  @Override
  public ReadAccessor registerRead(Object obj, long revision) {
    ReadAccessor ret = new ReadAccessor(obj, revision);
    active.put(ret, Boolean.TRUE);
    return ret;
  }

  @Override
  public long getLowestActiveRevision(long ceiling) {
    try {
      ReadAccessor first = active.firstKey();
      if (first != null) {
        return Math.min(ceiling, first.getRevision());
      }
    } catch (NoSuchElementException e) {
    }
    return ceiling;
  }

  @Override
  public long getLowestActiveRevision() {
    try {
      ReadAccessor first = active.firstKey();
      if (first != null) {
        return first.getRevision();
      }
    } catch (NoSuchElementException e) {
    }
    return -1l;
  }

  @Override
  public String dumpActive() {
    if (active.isEmpty()) {
      return "";
    }
    List<Long> ret = active.keySet().stream().map((e) -> e.getRevision()).collect(Collectors.toList());
    return ret.toString();
  }

}
