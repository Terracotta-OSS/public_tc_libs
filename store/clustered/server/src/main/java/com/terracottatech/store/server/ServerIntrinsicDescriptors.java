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
package com.terracottatech.store.server;

import com.terracottatech.sovereign.SovereignRecord;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.store.Cell;
import com.terracottatech.store.common.messages.StoreStructures;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicDescriptor;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicType;
import com.terracottatech.store.intrinsics.impl.RecordEquality;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static com.terracottatech.store.common.messages.StoreStructures.decodeCells;
import static com.terracottatech.store.common.messages.StoreStructures.decodeKey;
import static com.terracottatech.store.common.messages.StoreStructures.encodeKey;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_RECORD_EQUALS;

/**
 * Intrinsic descriptors overridden on the server-side.
 */
public class ServerIntrinsicDescriptors {

  public static final Map<IntrinsicType, IntrinsicDescriptor> OVERRIDDEN_DESCRIPTORS = new EnumMap<>(IntrinsicType.class);

  static {
    OVERRIDDEN_DESCRIPTORS.put(PREDICATE_RECORD_EQUALS, new IntrinsicDescriptor(IntrinsicCodec.INTRINSIC_DESCRIPTORS.get(PREDICATE_RECORD_EQUALS),
        (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec)
                -> encode(intrinsicEncoder, (RecordEquality<?>) intrinsic),
        (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec)
                -> decode(intrinsicDecoder))
    );
  }

  private static void encode(StructEncoder<?> intrinsicEncoder, RecordEquality<?> intrinsic) {
    SovereignRecord<?> record = (SovereignRecord) intrinsic.getRecord();
    encodeKey(intrinsicEncoder.struct("key"), record.getKey());
    intrinsicEncoder.int64("msn", record.getMSN());
    intrinsicEncoder.structs("cells", record, StoreStructures::encodeCell);
  }

  private static <K extends Comparable<K>> Intrinsic decode(StructDecoder<?> intrinsicDecoder) {
    K key = decodeKey(intrinsicDecoder.struct("key"));
    Long msn = intrinsicDecoder.int64("msn");
    Collection<Cell<?>> cells = decodeCells(intrinsicDecoder.structs("cells"));
    return new RecordEquality<>(new DupeSovereignRecord<>(msn, key, cells));
  }

  private static class DupeSovereignRecord<K extends Comparable<K>> implements SovereignRecord<K> {
    private final long msn;
    private final K key;
    private final Iterable<Cell<?>> cells;
    private volatile List<Cell<?>> materializedCells;

    DupeSovereignRecord(long msn, K key, Iterable<Cell<?>> cells) {
      this.msn = msn;
      this.key = key;
      this.cells = cells;
    }

    @Override
    public long getMSN() {
      return msn;
    }

    @Override
    public K getKey() {
      return key;
    }

    private List<Cell<?>> getMaterializedCells() {
      List<Cell<?>> ret = materializedCells;
      if (ret == null) {
        synchronized (this) {
          ret = materializedCells;
          if (ret == null) {
            List<Cell<?>> list = new ArrayList<>();
            cells.forEach(list::add);
            ret = materializedCells = list;
          }
        }
      }
      return ret;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof SovereignRecord) {
        SovereignRecord<?> other = (SovereignRecord) obj;

        if (!key.equals(other.getKey())) {
          return false;
        }

        List<Cell<?>> materializedCells = getMaterializedCells();
        materializedCells.sort(Comparator.comparing(cell -> cell.definition().name()));

        List<Cell<?>> otherMaterializedCells = new ArrayList<>();
        ((Iterable<Cell<?>>) other).forEach(otherMaterializedCells::add);
        otherMaterializedCells.sort(Comparator.comparing(cell -> cell.definition().name()));

        return materializedCells.equals(otherMaterializedCells);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, getMaterializedCells());
    }


    @Override
    public TimeReference<?> getTimeReference() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stream<SovereignRecord<K>> versions() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Cell<?>> iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException();
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
  }

}
