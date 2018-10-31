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

package com.terracottatech.store;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

public class TestRecord<K extends Comparable<K>> implements Record<K> {

  private final K key;
  private final CellSet cells;

  public TestRecord(K key, Iterable<Cell<?>> cells) {
    this.key = key;
    this.cells = new CellSet(cells);
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public int size() {
    return cells.size();
  }

  @Override
  public boolean isEmpty() {
    return cells.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return cells.contains(o);
  }

  @Override
  public Iterator<Cell<?>> iterator() {
    return cells.iterator();
  }

  @Override
  public Object[] toArray() {
    return cells.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return cells.toArray(a);
  }

  @Override
  public boolean add(Cell<?> cell) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return cells.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends Cell<?>> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TestRecord)) return false;
    TestRecord<?> that = (TestRecord<?>) o;
    return Objects.equals(key, that.key) &&
            Objects.equals(cells, that.cells);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, cells);
  }
}
