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
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.intrinsics.impl.InstallOperation;
import com.terracottatech.store.intrinsics.impl.RecordSameness;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * @author Ludovic Orban
 */
public class ClusteredConditionalReadWriteRecordAccessor<K extends Comparable<K>> extends ClusteredConditionalReadRecordAccessor<K> implements ConditionalReadWriteRecordAccessor<K> {

  ClusteredConditionalReadWriteRecordAccessor(K key, DatasetEntity<K> entity, Predicate<? super Record<K>> predicate) {
    super(key, entity, predicate);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Optional<Tuple<Record<K>, Record<K>>> update(UpdateOperation<? super K> transform) {
    if (predicate instanceof IntrinsicPredicate<?> && transform instanceof IntrinsicUpdateOperation<?>) {
      return Optional.ofNullable(entity.updateReturnTuple(key,
              (IntrinsicPredicate<? super Record<K>>) predicate,
              (IntrinsicUpdateOperation<? super K>)transform
      ));
    }

    while (true) {
      RecordImpl<K> oldRecord = entity.get(key);
      if (oldRecord != null && predicate.test(oldRecord)) {
        Iterable<Cell<?>> newCells = transform.apply((Record) oldRecord);
        Tuple<Record<K>, Record<K>> updated = entity.updateReturnTuple(key,
                new RecordSameness(oldRecord.getMSN(), key),
                new InstallOperation<>(newCells)
        );
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
    if (predicate instanceof IntrinsicPredicate<?>) {
      @SuppressWarnings("unchecked")
      IntrinsicPredicate<? super Record<K>> predicate = (IntrinsicPredicate<? super Record<K>>) this.predicate;
      return Optional.ofNullable(entity.deleteReturnRecord(key, predicate));
    }

    while (true) {
      RecordImpl<K> oldRecord = entity.get(key);
      if (oldRecord != null && predicate.test(oldRecord)) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        IntrinsicPredicate<? super Record<K>> predicate = new RecordSameness(oldRecord.getMSN(), key);
        Record<K> deletedRecord = entity.deleteReturnRecord(key, predicate);
        if (deletedRecord != null) {
          return Optional.of(deletedRecord);
        }
      } else {
        return Optional.empty();
      }
    }
  }
}
