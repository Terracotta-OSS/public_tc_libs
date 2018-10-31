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
package com.terracottatech.store.common.messages.intrinsics;

import com.terracottatech.store.Cell;
import com.terracottatech.store.common.messages.StoreStructures;
import com.terracottatech.store.internal.InternalRecord;
import com.terracottatech.store.internal.function.Functions;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicType;
import com.terracottatech.store.intrinsics.impl.RecordEquality;
import com.terracottatech.store.intrinsics.impl.RecordSameness;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.terracottatech.store.common.messages.StoreStructures.decodeCells;
import static com.terracottatech.store.common.messages.StoreStructures.decodeKey;
import static com.terracottatech.store.common.messages.StoreStructures.encodeKey;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_RECORD_EQUALS;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_RECORD_SAME;


public class TestIntrinsicDescriptors {

  @SuppressWarnings({"unchecked", "rawtypes", "serial"})
  public static final Map<IntrinsicType, IntrinsicDescriptor> OVERRIDDEN_DESCRIPTORS = new HashMap<IntrinsicType, IntrinsicDescriptor>() {{
    put(PREDICATE_RECORD_EQUALS, new IntrinsicDescriptor(IntrinsicCodec.INTRINSIC_DESCRIPTORS.get(PREDICATE_RECORD_EQUALS),
        (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
          DupeRecord<?> record = (DupeRecord) ((RecordEquality<?>) intrinsic).getRecord();
          Comparable<?> key = record.getKey();
          StoreStructures.encodeKey(intrinsicEncoder.struct("key"), key);
          intrinsicEncoder.int64("msn", record.getMSN());
          intrinsicEncoder.structs("cells", record, StoreStructures::encodeCell);
        },
        (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
          Comparable key = StoreStructures.decodeKey(intrinsicDecoder.struct("key"));
          Long msn = intrinsicDecoder.int64("msn");
          Collection<Cell<?>> cells = decodeCells(intrinsicDecoder.structs("cells"));
          return new RecordEquality<>(new DupeRecord<>(msn, key, cells));
        }));
    put(PREDICATE_RECORD_SAME, new IntrinsicDescriptor(IntrinsicCodec.INTRINSIC_DESCRIPTORS.get(PREDICATE_RECORD_SAME),
        (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
          RecordSameness<?> testRecordSameness = (RecordSameness<?>) intrinsic;
          encodeKey(intrinsicEncoder.struct("key"), testRecordSameness.getKey());
          intrinsicEncoder.int64("msn", testRecordSameness.getMSN());
        },
        (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
          Comparable key = decodeKey(intrinsicDecoder.struct("key"));
          Long msn = intrinsicDecoder.int64("msn");
          return new RecordSameness<>(msn, key);
        })
    );
  }};

  public static class DupeRecord<K extends Comparable<K>> implements InternalRecord<K> {
    private final long msn;
    private final K key;
    private final Collection<Cell<?>> cells;

    DupeRecord(long msn, K key, Collection<Cell<?>> cells) {
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

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DupeRecord) {
        DupeRecord<?> other = (DupeRecord) obj;

        if (!Objects.equals(key, other.getKey())) {
          return false;
        }

        List<Cell<?>> cellList = new ArrayList<>(cells);
        cellList.sort(Comparator.comparing(cell -> cell.definition().name()));

        List<Cell<?>> otherCellList = new ArrayList<>(other.cells);
        otherCellList.sort(Comparator.comparing(cell -> cell.definition().name()));

        return Objects.equals(cellList, otherCellList);
      }
      return false;
    }

    @Override
    public int hashCode() {
      List<Cell<?>> cellList = new ArrayList<>(cells);
      cellList.sort(Comparator.comparing(cell -> cell.definition().name()));
      return Objects.hash(key, cellList);
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
      return cells.iterator();
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
