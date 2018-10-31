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
package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.SovereignRecord;
import com.terracottatech.sovereign.VersionLimitStrategy;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;

/**
 * An immutable aggregation of the key, cells, and metadata for a single {@link com.terracottatech.store.Record Record}
 * instance.
 *
 * @author cschanck
 **/
public class SingleRecord<K extends Comparable<K>>
    implements SovereignPersistentRecord<K> {

  private final K key;
  protected final Map<String, Cell<?>> cells;
  private final TimeReference<?> timeReference;

  /**
   * Mutation Sequence Number (MSN) for this {@code Record}.  The MSN is updated for each
   * record addition and mutation in a {@code SovereignDataset}.
   */
  private final long msn;
  /**
   * The {@code VersionedRecord} instance of which this {@code SingleRecord} instance is
   * a member.
   */
  private final VersionedRecord<K> parent;

  public SingleRecord(VersionedRecord<K> parent, K key, TimeReference<?> timeReference, long msn, Map<String, Cell<?>> cells) {
    // TODO: Enforce parent != null?
    this.parent = parent;
    this.key = key;
    this.timeReference = timeReference;
    this.msn = msn;
    // TODO: Should this make a copy of the Cells?
    this.cells = Collections.unmodifiableMap(cells);
  }

  public SingleRecord(VersionedRecord<K> parent, K key, TimeReference<?> timeReference, long msn, Cell<?>... values) {
    this(parent, key, timeReference, msn, collectCells(Arrays.asList(values)));
  }

  public SingleRecord(VersionedRecord<K> parent, K key, TimeReference<?> timeReference, long msn, Iterable<Cell<?>> values) {
    this(parent, key, timeReference, msn, collectCells(values));
  }

  /**
   * Assembles the {@code Cell} members for this {@code SingleRecord}.
   * For {@code Cell}s for which {@link CellDefinition#name() Cell.description().name()} are
   * equal, only the <i>last</i> cell is retained.
   *
   * @param cells an {@code Iterable} containing the {@code Cell} values
   *
   * @return a new {@code Map} containing references to the {@code Cell} instances
   *      obtained from the {@code values} iterator
   */
  private static Map<String, Cell<?>> collectCells(final Iterable<Cell<?>> cells) {
    Iterator<Cell<?>> values = cells.iterator();
    if (values.hasNext()) {
      final Map<String, Cell<?>> cellMap = new LinkedHashMap<>();
      while (values.hasNext()) {
        final Cell<?> cell = values.next();
        cellMap.put(cell.definition().name(), cell);
      }
      return cellMap;
    } else {
      return Collections.emptyMap();
    }
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public <T> Optional<T> get(CellDefinition<T> cellDefinition) {
    Objects.requireNonNull(cellDefinition);
    Cell<?> val = cells.get(cellDefinition.name());
    try {
      return val == null ? empty() : of(cellDefinition.type().getJDKType().cast(val.value()));
    } catch (ClassCastException cast) {
      return empty();
    }
  }

  @Override
  public Optional<?> get(String name) {
    Objects.requireNonNull(name);
    Cell<?> cell = this.cells.get(name);
    return ( cell == null ? empty() : of(cell.value()) );
  }

  @Override
  public TimeReference<?> getTimeReference() {
    return this.timeReference;
  }

  @Override
  public Stream<SovereignRecord<K>> versions() {
    return parent.elements().stream()
        .filter(r -> r.getMSN() <= this.msn)
        .map(Function.identity());
  }

  @Override
  public Iterator<Cell<?>> iterator() {
    return cells.values().iterator();
  }

  @Override
  public Spliterator<Cell<?>> spliterator() {
    return Spliterators.spliterator(this, Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.IMMUTABLE);
  }

  @Override
  public int size() {
    return this.cells.size();
  }

  @Override
  public boolean isEmpty() {
    return this.cells.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    requireNonNull(o);
    return this.cells.values().contains(o);
  }

  @Override
  public Object[] toArray() {
    return this.cells.values().toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return this.cells.values().toArray(a);
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
    return this.cells.values().containsAll(checkNulls(c));
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
  public boolean removeIf(Predicate<? super Cell<?>> filter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PersistentMemoryLocator getLocation() {
    return PersistentMemoryLocator.INVALID;
  }

  @Override
  public void setLocation(PersistentMemoryLocator loc) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Cell<?>> cells() {
    return this.cells;
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
  public String toString() {
    return "SingleRecord{" +
      "key=" + key +
      ", timestamp=" + timeReference +
      ", msn=" + msn +
      ", cells=" + cells.values() +
      '}';
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

  /**
   * Provides a more traditional {@code equals} implementation that compares the
   * values of each field between two {@code SingleRecord} instances.  This method
   * does <b>not</b> test parentage other than presence.
   * <p>
   * This method very expensive and is intended for use only in testing scenarios.
   *
   * @param other the {@code SingleRecord} instance to compare to {@code this}
   * @return {@code true} if all field values are equal; {@code false} otherwise
   */
  @Override
  public boolean deepEquals(SovereignPersistentRecord<K> other) {
    if (!this.equals(other)) {
      return false;
    }
    if(!(other instanceof SingleRecord)) {
      return false;
    }

    SingleRecord<K> that = (SingleRecord<K>) other;

    if ((this.parent != null && that.parent == null)
        || (this.parent == null && that.parent != null)) {
      return false;   // Only one has a parent
    }

    if (this.timeReference.compareTo(that.timeReference) != 0) {
      return false;
    }

    if (this.cells.size() != that.cells.size()) {
      return false;
    }

    for (final Map.Entry<String, Cell<?>> cellEntry : this.cells.entrySet()) {
      final String cellName = cellEntry.getKey();
      if (!cellEntry.getValue().equals(that.cells.get(cellName))) {
        return false;
      }
    }

    return true;
  }

  private static <E> Collection<E> checkNulls(Collection<E> c) {
    requireNonNull(c, "collection");
    c.forEach(e -> requireNonNull(e, "An element may not be null"));
    return c;
  }

  @Override
  public void prune(TimeReference<?> z, BiFunction<TimeReference<?>, TimeReference<?>, VersionLimitStrategy.Retention> function) {
    throw new UnsupportedOperationException();
  }
}
