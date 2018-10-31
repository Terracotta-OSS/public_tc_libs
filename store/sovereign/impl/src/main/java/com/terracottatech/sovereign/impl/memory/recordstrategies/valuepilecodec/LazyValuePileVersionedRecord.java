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
import com.terracottatech.store.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * @author cschanck
 **/
class LazyValuePileVersionedRecord<K extends Comparable<K>>
  implements SovereignPersistentRecord<K> {

  private final Type<K> keyType;
  private final ArrayList<LazyValuePileSingleRecord<K>> versions;
  private final int keyOffset;
  private final RandomValuePileReader reader;
  private final DatasetSchemaImpl schema;
  private PersistentMemoryLocator loc;
  private volatile K key = null;

  public LazyValuePileVersionedRecord(RandomValuePileReader reader,
                                      DatasetSchemaImpl schema,
                                      Type<K> keyType,
                                      int keyOffset,
                                      LazyValuePileSingleRecord<K>[] singles) {
    this.reader = reader;
    this.schema = schema;
    this.keyType = keyType;
    this.keyOffset = keyOffset;
    this.versions = new ArrayList<>(singles.length);
    for (int i = 0; i < singles.length; i++) {
      LazyValuePileSingleRecord<K> sr = singles[i];
      sr.setParent(this);
      versions.add(sr);
    }
  }

  public RandomValuePileReader getReader() {
    return reader;
  }

  public DatasetSchemaImpl getSchema() {
    return schema;
  }

  private LazyValuePileSingleRecord<?> first() {
    return versions.get(0);
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
  public PersistentMemoryLocator getLocation() {
    return loc;
  }

  @Override
  public void setLocation(PersistentMemoryLocator loc) {
    this.loc = loc;
  }

  @SuppressWarnings("unchecked")
  @Override
  public K getKey() {
    if (key == null) {
      key = (K) ValuePileBufferReader.readCellValueOnly(reader, keyType, keyOffset);
    }
    return key;
  }

  @Override
  public Optional<?> get(String name) {
    return first().get(name);
  }

  @Override
  public <T> Optional<T> get(CellDefinition<T> cellDefinition) {
    return first().get(cellDefinition);
  }

  @Override
  public Map<String, Cell<?>> cells() {
    return first().cells();
  }

  @Override
  public List<SovereignPersistentRecord<K>> elements() {
    return new ArrayList<>(versions);
  }

  @Override
  public long getMSN() {
    return first().getMSN();
  }

  @Override
  public boolean deepEquals(SovereignPersistentRecord<K> that) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void prune(TimeReference<?> now, BiFunction<TimeReference<?>, TimeReference<?>, VersionLimitStrategy.Retention> versionLimitFunction) {

    if (versions.size() == 0) {
      return;
    }

    ListIterator<LazyValuePileSingleRecord<K>> iterator = versions.listIterator(versions.size());
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
    return first().iterator();
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

  @Override
  public String toString() {
    return toString(true);
  }

  public final String toString(boolean materialize) {
    StringBuilder sb = new StringBuilder(1024);
    sb.append("LazyVersionedRecord{");
    sb.append("records=");
    if (versions.isEmpty()) {
      sb.append("[]");
    } else {
      sb.append('[');
      Iterator<LazyValuePileSingleRecord<K>> it = versions.iterator();
      sb.append(it.next().toString(materialize));
      while (it.hasNext()) {
        sb.append(", ").append(it.next().toString(materialize));
      }
      sb.append(']');
    }
    sb.append('}');
    return sb.toString();
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

}
