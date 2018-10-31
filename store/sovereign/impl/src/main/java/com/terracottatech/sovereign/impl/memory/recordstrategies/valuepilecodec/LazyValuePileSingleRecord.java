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
package com.terracottatech.sovereign.impl.memory.recordstrategies.valuepilecodec;

import com.terracottatech.sovereign.SovereignRecord;
import com.terracottatech.sovereign.VersionLimitStrategy;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.common.valuepile.RandomValuePileReader;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;

/**
 * @author cschanck
 **/
class LazyValuePileSingleRecord<K extends Comparable<K>>
  implements SovereignPersistentRecord<K> {

  private final long msn;
  private final TimeReference<?> timeRef;
  private LazyCell<?>[] lazyArray;
  private volatile int nextRenderedCell;
  private LazyValuePileVersionedRecord<K> parent;
  private PersistentMemoryLocator loc;
  private Map<String, Cell<?>> cache = new ConcurrentHashMap<>(8);

  public LazyValuePileSingleRecord(long msn, TimeReference<?> reference) {
    this.msn = msn;
    this.timeRef = reference;
    this.nextRenderedCell = 0;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public void setLazyArray(LazyCell<?>[] lazyArray) {
    this.lazyArray = lazyArray;
    cache.clear();
    nextRenderedCell = 0;
  }

  public void setParent(LazyValuePileVersionedRecord<K> parent) {
    this.parent = parent;
  }

  @Override
  public TimeReference<?> getTimeReference() {
    return timeRef;
  }

  @SuppressFBWarnings("VO_VOLATILE_INCREMENT")
  private Cell<?> faultIn(String name) {
    Cell<?> ret = cache.get(name);
    if (ret != null) {
      return ret;
    }
    for (int tmp = nextRenderedCell; tmp < lazyArray.length; ) {
      LazyCell<?> c = lazyArray[tmp];
      cache.put(c.definition().name(), c);
      nextRenderedCell = ++tmp;
      if (c.definition().name().equals(name)) {
        return c;
      }
    }
    return null;
  }

  @Override
  public Stream<SovereignRecord<K>> versions() {
    return parent.elements().stream().filter(r -> r.getMSN() <= this.msn).map(Function.identity());
  }

  @Override
  public PersistentMemoryLocator getLocation() {
    return loc;
  }

  @Override
  public void setLocation(PersistentMemoryLocator loc) {
    this.loc = loc;
  }

  @Override
  public K getKey() {
    return parent.getKey();
  }

  @Override
  public Optional<?> get(String name) {
    Cell<?> p = faultIn(name);
    if(p!=null) {
      return Optional.of(p.value());
    }
    return empty();
  }

  @Override
  public <T> Optional<T> get(CellDefinition<T> cellDefinition) {
    @SuppressWarnings("unchecked")
    Cell<T> p = (Cell<T>) faultIn(cellDefinition.name());
    if (p != null && cellDefinition.type().asEnum() == p.definition().type().asEnum()) {
      return Optional.of(p.value());
    }
    return empty();
  }

  @Override
  public Map<String, Cell<?>> cells() {
    for (int i = nextRenderedCell; i < lazyArray.length; ) {
      LazyCell<?> c = lazyArray[i];
      nextRenderedCell = ++i;
      cache.put(c.definition().name(), c);
    }
    return Collections.unmodifiableMap(cache);
  }

  @Override
  public List<SovereignPersistentRecord<K>> elements() {
    return Collections.singletonList(this);
  }

  @Override
  public long getMSN() {
    return msn;
  }

  @Override
  public boolean deepEquals(SovereignPersistentRecord<K> that) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void prune(TimeReference<?> z, BiFunction<TimeReference<?>, TimeReference<?>, VersionLimitStrategy.Retention> function) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return this.cells().size();
  }

  @Override
  public boolean isEmpty() {
    return this.cells().isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    requireNonNull(o);
    return this.cells().values().contains(o);
  }

  @Override
  public Iterator<Cell<?>> iterator() {
    List<Cell<?>> ret = Arrays.asList(lazyArray);
    return ret.iterator();
  }

  @Override
  public Object[] toArray() {
    return this.cells().values().toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return this.cells().values().toArray(a);
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
    throw new UnsupportedOperationException();
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

  public RandomValuePileReader getReader() {
    return parent.getReader();
  }

  public DatasetSchemaImpl getSchema() {
    return parent.getSchema();
  }

  public int cellCount() {
    return lazyArray.length;
  }

  @Override
  public String toString() {
    return toString(true);
  }

  /**
   * A {@code toString} variant permitting a choice of materializing lazy cell content.
   * {@code toString(false)} is used for internal debugging contexts.
   * @param materialize if {@code true}, materializes "lazy" content; if {@code false} "lazy"
   *                    content is not materialized
   * @return a {@code String} representation of this {@code Record}
   */
  public final String toString(boolean materialize) {
    StringBuilder sb = new StringBuilder(1024);
    sb.append("LazySingleRecord{");
    sb.append("key=").append(getKey());
    sb.append(", timestamp=").append(getTimeReference());
    sb.append(", msn=").append(getMSN());
    sb.append(", cells=");
    if (lazyArray.length == 0) {
      sb.append("[]");
    } else {
      sb.append('[');
      sb.append(lazyArray[0].toString(materialize));
      for (int i = 1; i < lazyArray.length; i++) {
        sb.append(", ").append(lazyArray[i].toString(materialize));
      }
      sb.append(']');
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || ((!(o instanceof SovereignPersistentRecord)))) {
      return false;
    }
    final SovereignPersistentRecord<? extends Comparable<?>> that = (SovereignPersistentRecord<? extends Comparable<?>>) o;

    return Objects.equals(getMSN(), that.getMSN()) &&
      Objects.equals(getKey(), that.getKey());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKey(), getMSN());
  }
}
