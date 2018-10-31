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
import com.terracottatech.store.ConditionalReadWriteRecordAccessor;
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import com.terracottatech.store.intrinsics.impl.InstallOperation;
import com.terracottatech.store.intrinsics.impl.RecordSameness;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * @author Ludovic Orban
 */
public class ClusteredReadWriteRecordAccessor<K extends Comparable<K>> extends ClusteredReadRecordAccessor<K> implements ReadWriteRecordAccessor<K> {

  ClusteredReadWriteRecordAccessor(K key, DatasetEntity<K> entity) {
    super(key, entity);
  }

  @Override
  public ConditionalReadWriteRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
    return new ClusteredConditionalReadWriteRecordAccessor<>(key, entity, predicate);
  }

  @Override
  public Optional<Record<K>> add(Iterable<Cell<?>> cells) {
    return Optional.ofNullable(entity.addReturnRecord(key, cells));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Optional<Tuple<Record<K>, Record<K>>> update(UpdateOperation<? super K> transform) {
    if (transform instanceof IntrinsicUpdateOperation<?>) {
      return Optional.ofNullable(entity.updateReturnTuple(key,
              AlwaysTrue.alwaysTrue(),
              (IntrinsicUpdateOperation<? super K>)transform
      ));
    }

    while (true) {
      RecordImpl<K> oldRecord = entity.get(key);
      if (oldRecord != null) {
        Iterable<Cell<?>> newCells = transform.apply((Record) oldRecord);
        Tuple<Record<K>, Record<K>> updated = entity.updateReturnTuple(key,
                new RecordSameness(oldRecord.getMSN(), oldRecord.getKey()),
                new InstallOperation<>(newCells));
        if (updated != null) {
          return Optional.of(updated);
        }
      } else {
        return Optional.empty();
      }
    }
  }

  @Override
  public Optional<Record<K>> delete() {
    return Optional.ofNullable(entity.deleteReturnRecord(key, AlwaysTrue.alwaysTrue()));
  }

}
