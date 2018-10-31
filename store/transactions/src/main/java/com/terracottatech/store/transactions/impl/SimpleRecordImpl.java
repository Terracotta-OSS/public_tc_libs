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
package com.terracottatech.store.transactions.impl;

import com.terracottatech.store.Cell;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Record;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

class SimpleRecordImpl<K extends Comparable<K>> implements Record<K> {

  private final K key;
  private final CellSet cellSet;

  SimpleRecordImpl(K key, Iterable<Cell<?>> cellIterable) {
    this.key = key;
    this.cellSet = new CellSet(cellIterable);
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public boolean removeIf(Predicate<? super Cell<?>> filter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Spliterator<Cell<?>> spliterator() {
    return cellSet.spliterator();
  }

  @Override
  public Stream<Cell<?>> stream() {
    return cellSet.stream();
  }

  @Override
  public Stream<Cell<?>> parallelStream() {
    return cellSet.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super Cell<?>> action) {
    cellSet.forEach(action);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SimpleRecordImpl) {
      SimpleRecordImpl<?> other = (SimpleRecordImpl<?>) obj;
      return key.equals(other.key) && cellSet.equals(other.cellSet);
    }
    return false;
  }

  @Override
  public String toString() {
    return "SimpleRecordImpl{"
      + "key=" + key
      + ", cells=" + cellSet.toString()
      + '}';
  }

  @Override
  public int size() {
    return cellSet.size();
  }

  @Override
  public boolean isEmpty() {
    return cellSet.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return cellSet.contains(o);
  }

  @Override
  public Iterator<Cell<?>> iterator() {
    return cellSet.iterator();
  }

  @Override
  public Object[] toArray() {
    return cellSet.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return cellSet.toArray(a);
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
    return cellSet.containsAll(c);
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
}
