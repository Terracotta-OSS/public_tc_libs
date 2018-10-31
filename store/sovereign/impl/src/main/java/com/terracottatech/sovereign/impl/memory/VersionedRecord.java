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
import com.terracottatech.sovereign.impl.utils.TriPredicate;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A {@link SovereignPersistentRecord} implementation supporting the retention of multiple versions.
 * The record versions are {@link SingleRecord} instances.
 *
 * @param <K> the key type of this record
 *
 * @author cschanck
 */
public class VersionedRecord<K extends Comparable<K>>
    implements SovereignPersistentRecord<K> {

  /**
   * The {@code SingleRecord} instances comprising the versions available for this {@code VersionedRecord}.
   * The first element in this list is the most recent version of the record; the remaining elements are
   * ordered by increasing age.
   * <p>
   * New versions ({@code SingleRecord} instances) should not added directly to this list;
   */
  protected final ArrayList<SovereignPersistentRecord<K>> versions = new ArrayList<>();
  private transient PersistentMemoryLocator locator = null;

  public static <KK extends Comparable<KK>>
  VersionedRecord<KK> single(KK key, TimeReference<?> timeReference, long msn, Iterable<Cell<?>> values) {
    VersionedRecord<KK> ret = new VersionedRecord<>();
    SingleRecord<KK> single = new SingleRecord<>(ret, key, timeReference, msn, values);
    ret.versions.add(single);
    return ret;
  }

  public VersionedRecord() {
  }

  public VersionedRecord(SovereignPersistentRecord<K> old, Iterable<Cell<?>> cells, TimeReference<?> timeReference, long msn,
                         TriPredicate<TimeReference<?>, TimeReference<?>, Integer> versionAllowed) {
    super();
    K key = old.getKey();
    SingleRecord<K> seed = new SingleRecord<>(this, key, timeReference, msn, cells);
    this.versions.add(seed);
    for (int i = 0; i < old.elements().size(); i++) {
      SovereignPersistentRecord<K> ver = old.elements().get(i);
      if (versionAllowed.test(timeReference, ver.getTimeReference(), i + 1)) {
        this.versions.add(ver);
      }
    }
  }

  private SovereignPersistentRecord<K> first() {
    return versions.get(0);
  }

  @Override
  public long getMSN() {
    return first().getMSN();
  }

  /**
   * This is an uber expensive method useful only for testing.
   * @param that
   * @return
   */
  @Override
  public boolean deepEquals(SovereignPersistentRecord<K> that) {
    if (elements().size() != that.elements().size()) {
      return false;
    }
    if (!this.equals(that)) {
      return false;
    }
    Iterator<SovereignPersistentRecord<K>> left = elements().iterator();
    Iterator<? extends SovereignPersistentRecord<K>> right = that.elements().iterator();
    while(left.hasNext()) {
      if(!left.next().deepEquals(right.next())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public K getKey() {
    return first().getKey();
  }

  @Override
  public <T> Optional<T> get(CellDefinition<T> cellDefinition) {
    return first().get(cellDefinition);
  }

  @Override
  public Optional<?> get(String name) {
    return first().get(name);
  }

  @Override
  public TimeReference<?> getTimeReference() {
    return first().getTimeReference();
  }

  @Override
  public Stream<SovereignRecord<K>> versions() {
    return this.versions.stream().map(Function.identity());
  }

  @Override
  public Iterator<Cell<?>> iterator() {
    return getActiveCells().iterator();
  }

  /**
   * Gets the collection of "active" cells for this {@code VersionedRecord}.
   * @return a new {@code Collection} of cells
   */
  private Collection<Cell<?>> getActiveCells() {
    return first().cells().values();
  }

  @Override
  public int size() {
    return this.getActiveCells().size();
  }

  @Override
  public boolean isEmpty() {
    return this.getActiveCells().isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return this.getActiveCells().contains(o);
  }

  @Override
  public Object[] toArray() {
    return this.getActiveCells().toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return this.getActiveCells().toArray(a);
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
    return this.getActiveCells().containsAll(c);
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
    return locator;
  }

  @Override
  public void setLocation(PersistentMemoryLocator loc) {
    locator = loc;
  }

  @Override
  public Map<String, Cell<?>> cells() {
    return this.first().cells();
  }

  @Override
  public List<SovereignPersistentRecord<K>> elements() {
    return this.versions;
  }

  /**
   * Appends a {@code SingleRecord} to the version collection of this {@code VersionedRecord}.
   * The {@code msn} of the appended version must be less than or equal to the last version currently in the
   * collection.
   *
   * @param version the new {@code SingleRecord} to append
   */
  public void addLast(final SingleRecord<K> version) {
    Objects.requireNonNull(version);

    if (this.versions.isEmpty()) {
      this.versions.add(version);
    } else {
      final long lastMsn = this.versions.get(this.versions.size() - 1).getMSN();
      if (lastMsn < version.getMSN()) {
        throw new IllegalArgumentException("Can not add version with MSN not more than " + lastMsn + ": " + version.getMSN());
      }
      this.versions.add(version);
    }
  }

  @Override
  public String toString() {
    return "VersionedRecord{" +
      "versions=" + versions +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null){
      return false;
    }

    if(!(o instanceof SovereignRecord)){
      return false;
    }

    @SuppressWarnings("unchecked")
    SovereignRecord<K> record = (SovereignRecord<K>) o;

    Iterator<SovereignRecord<K>> iter1 = versions().iterator();
    Iterator<? extends SovereignRecord<?>> iter2 = record.versions().iterator();

    while(iter1.hasNext() && iter2.hasNext()) {
      if (!(iter1.next().equals(iter2.next()))) {
        return false;
      }
    }

    if(iter1.hasNext() || iter2.hasNext()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return versions != null ? versions.hashCode() : 0;
  }

  /**
   * Provides a more traditional {@code equals} implementation that compares the
   * values of each field between two {@code VersionedRecord} instances.
   * <p>
   * This method very expensive and is intended for use only in testing scenarios.
   *
   * @param that the {@code VersionedRecord} instance to compare to {@code this}
   *
   * @return {@code true} if all field values are equal; {@code false} otherwise
   *
   */
  public boolean deepEquals(final VersionedRecord<K> that) {
    if (this == that) {
      return true;
    }
    if (that == null || this.getClass() != that.getClass()) {
      return false;
    }

    ListIterator<SovereignPersistentRecord<K>> theseRecords = this.versions.listIterator();
    ListIterator<SovereignPersistentRecord<K>> thoseRecords = that.versions.listIterator();
    while (theseRecords.hasNext() && thoseRecords.hasNext()) {
      if (!theseRecords.next().deepEquals(thoseRecords.next())) {
        return false;
      }
    }
    return theseRecords.hasNext() == thoseRecords.hasNext();
  }

  /**
   * Reduces the versions present in this {@code VersionedRecord} based on the time reference and
   * function provided.
   * <p>
   * This method leaves at least one version in this record.
   *
   * @param now the {@code TimeReference} value against which the version is compared
   * @param versionLimitFunction the {@code BiFunction} determining if a version is retained or dropped;
   *                             the first argument to {@code versionLimitFunction} is {@code now} and
   *                             the second argument is the value of
   *                             {@link SingleRecord#getTimeReference() getTimeReference} for the
   *                             version
   */
  public void prune(TimeReference<?> now, BiFunction<TimeReference<?>, TimeReference<?>, VersionLimitStrategy.Retention> versionLimitFunction) {

    if (versions.size() == 0) {
      return;
    }

    ListIterator<SovereignPersistentRecord<K>> iterator = versions.listIterator(versions.size());
    endScan:
    while (iterator.hasPrevious() && versions.size() > 1) {
      final SovereignPersistentRecord<K> record = iterator.previous();
      switch (versionLimitFunction.apply(now, record.getTimeReference())) {
        case DROP:
          iterator.remove();
          break;
        case KEEP:
          // retained but scan continues
          break;
        case FINISH:
          // retained and scan is complete
          break endScan;
        default:
          throw new IllegalStateException("Unexpected VersionLimitStrategy.Retention value");
      }
    }
  }
}
