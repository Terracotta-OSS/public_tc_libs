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
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.async.AsyncDatasetWriterReader;
import com.terracottatech.store.async.ExecutorDrivenAsyncDatasetWriterReader;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import com.terracottatech.store.intrinsics.impl.InstallOperation;
import com.terracottatech.store.intrinsics.impl.RecordSameness;
import com.terracottatech.store.stream.MutableRecordStream;

import java.util.concurrent.ForkJoinPool;

public class ClusteredDatasetWriterReader<K extends Comparable<K>> extends ClusteredDatasetReader<K> implements DatasetWriterReader<K> {

  public ClusteredDatasetWriterReader(DatasetEntity<K> entity) {
    super(entity);
  }

  @Override
  public boolean add(K key, Iterable<Cell<?>> cells) {
    return entity.add(key, cells);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public boolean update(K key, UpdateOperation<? super K> transform) {
    if (transform instanceof IntrinsicUpdateOperation<?>) {
      return entity.update(key,
              AlwaysTrue.alwaysTrue(),
              (IntrinsicUpdateOperation<? super K>)transform
      );
    }

    while (true) {
      RecordImpl<K> oldRecord = entity.get(key);
      if (oldRecord != null) {
        Iterable<Cell<?>> newCells = transform.apply((Record) oldRecord);
        if (entity.update(key,
                new RecordSameness(oldRecord.getMSN(), key),
                new InstallOperation<>(newCells)
        )) {
          return true;
        }
      } else {
        return false;
      }
    }
  }

  @Override
  public boolean delete(K key) {
    return entity.delete(key, AlwaysTrue.alwaysTrue());
  }

  @Override
  public ReadWriteRecordAccessor<K> on(K key) {
    return new ClusteredReadWriteRecordAccessor<>(key, entity);
  }

  @Override
  public MutableRecordStream<K> records() {
    return entity.mutableStream();
  }

  @Override
  public AsyncDatasetWriterReader<K> async() {
    return new ExecutorDrivenAsyncDatasetWriterReader<>(this, ForkJoinPool.commonPool());
  }
}
