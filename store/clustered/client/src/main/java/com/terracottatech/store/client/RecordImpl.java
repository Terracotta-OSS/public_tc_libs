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
package com.terracottatech.store.client;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.common.messages.RecordData;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.internal.InternalRecord;
import com.terracottatech.store.intrinsics.impl.RecordEquality;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class RecordImpl<K extends Comparable<K>> implements InternalRecord<K> {
  private final long msn;
  private final K key;
  private final Iterable<Cell<?>> cellIterable;
  private volatile Map<CellDefinition<?>, Object> cellDefinitionMap;
  private volatile Map<String, Object> cellNameMap;
  private volatile List<Cell<?>> materializedCells;

  public RecordImpl(Long msn, K key, Iterable<Cell<?>> cellIterable) {
    this.msn = Objects.requireNonNull(msn);
    this.key = Objects.requireNonNull(key);
    this.cellIterable = Objects.requireNonNull(cellIterable);
  }

  public RecordImpl(RecordData<K> data) {
    this(data.getMsn(), data.getKey(), data.getCells());
  }

  @Override
  public long getMSN() {
    return msn;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public <T> Optional<T> get(CellDefinition<T> cellDefinition) {
    Map<CellDefinition<?>, Object> cellDefinitionMap = getCellDefinitionMap();

    // It must be a T rather than a different type because the
    // CellDefinition matched in type
    @SuppressWarnings("unchecked")
    T result = (T) cellDefinitionMap.get(cellDefinition);
    return Optional.ofNullable(result);
  }

  @Override
  public Optional<?> get(String name) {
    Map<String, Object> cellNameMap = getCellNameMap();

    Object value = cellNameMap.get(name);
    return Optional.ofNullable(value);
  }

  @Override
  public int size() {
    return getCellNameMap().size();
  }

  @Override
  public boolean isEmpty() {
    return getCellNameMap().isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    requireNonNull(o);
    return getMaterializedCells().contains(o);
  }

  @Override
  public Iterator<Cell<?>> iterator() {
    return cellIterable.iterator();
  }

  @Override
  public Object[] toArray() {
    return getMaterializedCells().toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return getMaterializedCells().toArray(a);
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
    return getMaterializedCells().containsAll(checkNull(c));
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
  public boolean removeIf(Predicate<? super Cell<?>> filter) {
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
  public Spliterator<Cell<?>> spliterator() {
    return Spliterators.spliterator(this, Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.IMMUTABLE);
  }

  private static <E> Collection<E> checkNull(Collection<E> c) {
    requireNonNull(c, "collection");
    c.forEach(e -> requireNonNull(e, "An element may not be null"));
    return c;
  }

  private Map<CellDefinition<?>, Object> getCellDefinitionMap() {
    Map<CellDefinition<?>, Object> ret = cellDefinitionMap;
    if (ret == null) {
      synchronized (this) {
        ret = cellDefinitionMap;
        if (ret == null) {
          ret = new HashMap<>();
          for (Cell<?> cell : cellIterable) {
            ret.put(cell.definition(), cell.value());
          }
          cellDefinitionMap = ret;
        }
      }
    }
    return ret;
  }

  private Map<String, Object> getCellNameMap() {
    Map<String, Object> ret = cellNameMap;
    if (ret == null) {
      synchronized (this) {
        ret = cellNameMap;
        if (ret == null) {
          ret = new HashMap<>();
          for (Cell<?> cell : cellIterable) {
            ret.put(cell.definition().name(), cell.value());
          }
          cellNameMap = ret;
        }
      }
    }
    return ret;
  }

  private List<Cell<?>> getMaterializedCells() {
    List<Cell<?>> ret = materializedCells;
    if (ret == null) {
      synchronized (this) {
        ret = materializedCells;
        if (ret == null) {
          List<Cell<?>> list = new ArrayList<>();
          cellIterable.forEach(list::add);
          ret = materializedCells = list;
        }
      }
    }
    return ret;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RecordImpl) {
      RecordImpl<?> other = (RecordImpl) obj;
      return key.equals(other.key) && size() == other.size() && containsAll(other);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    Iterator<Cell<?>> i = iterator();
    while (i.hasNext()) {
      Cell<?> obj = i.next();
      if (obj != null)
        hash += obj.hashCode();
    }
    return hash;
  }

  @Override
  public BuildablePredicate<? super Record<K>> getEqualsPredicate() {
    return new RecordEquality<>(this);
  }

  @Override
  public String toString() {
    return "RecordImpl{"
        + "key=" + key
        + ", msn=" + msn
        + ", cells=" + getMaterializedCells().toString()
        + '}';
  }

  public static Record<?> toRecord(RecordData<?> data) {
    return new RecordImpl<>(data);
  }

}
