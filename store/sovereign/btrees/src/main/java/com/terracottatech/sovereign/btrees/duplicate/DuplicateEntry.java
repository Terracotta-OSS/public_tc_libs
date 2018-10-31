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
package com.terracottatech.sovereign.btrees.duplicate;

import java.util.AbstractList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author cschanck
 */
public class DuplicateEntry extends AbstractList<Long> {
  private Long key;
  private List<Long> values;

  public DuplicateEntry(long key, LongList values) {
    this.key = key;
    this.values = values.asList();
  }

  public DuplicateEntry(Long l) {
    key = l;
    values = Collections.emptyList();
  }

  public DuplicateEntry(Long l, long v) {
    key = l;
    values = Collections.singletonList((Long) v);
  }

  public long getKey() {
    return key;
  }

  @Override
  public Long get(int index) {
    return values.get(index);
  }

  @Override
  public Iterator<Long> iterator() {
    return values.iterator();
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public String toString() {
    return "DuplicateEntry{" +
      "key=" + key +
      ", values=" + values +
      '}';
  }
}
