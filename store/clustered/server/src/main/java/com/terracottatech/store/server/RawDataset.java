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

import com.tc.classloader.CommonComponent;
import com.terracottatech.sovereign.DatasetSchema;
import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignRecord;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.description.SovereignDatasetDescription;
import com.terracottatech.sovereign.exceptions.RecordLockedException;
import com.terracottatech.sovereign.exceptions.SovereignExtinctionException;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.memory.AbstractRecordContainer;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;

import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

@CommonComponent
public class RawDataset<K extends Comparable<K>> implements SovereignDataset<K> {
  private final SovereignDatasetImpl<K> dataset;
  private volatile boolean becameActive = false;

  public RawDataset(SovereignDataset<K> dataset) {
    this.dataset = (SovereignDatasetImpl<K>) dataset;
  }

  public SovereignDataset<K> promoteToActive() {
    testIsActive();
    becameActive = true;
    dataset.getContainer().getShards().forEach((AbstractRecordContainer<?> x) -> x.getBufferContainer().finishRestart());
    return this.dataset;
  }

  private void testIsActive() {
    if (becameActive) {
      throw new IllegalStateException("Already promoted to Active");
    }
  }

  public SovereignDatasetImpl<K> getDataset() {
    testIsActive();
    return dataset;
  }

  @Override
  public Type<K> getType() {
    testIsActive();
    return this.dataset.getType();
  }

  @Override
  public TimeReferenceGenerator<? extends TimeReference<?>> getTimeReferenceGenerator() {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public SovereignRecord<K> add(Durability durability, K key, Iterable<Cell<?>> cells) {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public void applyMutation(Durability durability, K key, Predicate<? super Record<K>> predicate, Function<? super Record<K>, Iterable<Cell<?>>> transform) {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public <T> Optional<T> applyMutation(Durability durability, K key, Predicate<? super Record<K>> predicate, Function<? super Record<K>, Iterable<Cell<?>>> transform, BiFunction<? super Record<K>, ? super Record<K>, T> output) {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public Consumer<Record<K>> applyMutation(Durability durability, Function<? super Record<K>, Iterable<Cell<?>>> transform) {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public <T> Function<Record<K>, T> applyMutation(Durability durability, Function<? super Record<K>, Iterable<Cell<?>>> transform, BiFunction<? super Record<K>, ? super Record<K>, T> output) {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public Consumer<Record<K>> delete(Durability durability) {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public <T> Function<Record<K>, T> delete(Durability durability, Function<? super Record<K>, T> output) {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public SovereignRecord<K> delete(Durability durability, K key) {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public SovereignRecord<K> delete(Durability durability, K key, Predicate<? super Record<K>> predicate) {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public SovereignRecord<K> tryAdd(Durability durability, K key, Iterable<Cell<?>> cells)
      throws RecordLockedException {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public void tryApplyMutation(Durability durability, K key, Predicate<? super Record<K>> predicate, Function<? super Record<K>, Iterable<Cell<?>>> transform)
      throws RecordLockedException {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public <T> Optional<T> tryApplyMutation(Durability durability, K key, Predicate<? super Record<K>> predicate, Function<? super Record<K>, Iterable<Cell<?>>> transform, BiFunction<? super Record<K>, ? super Record<K>, T> output)
      throws RecordLockedException {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public SovereignRecord<K> tryDelete(Durability durability, K key) throws RecordLockedException {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public SovereignRecord<K> tryDelete(Durability durability, K key, Predicate<? super Record<K>> predicate)
      throws RecordLockedException {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public SovereignRecord<K> get(K key) {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public SovereignRecord<K> tryGet(K key) {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public RecordStream<K> records() {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public SovereignIndexing getIndexing() {
    testIsActive();
    return this.dataset.getIndexing();
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public boolean isDisposed() {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public UUID getUUID() {
    testIsActive();
    return this.dataset.getUUID();
  }

  @Override
  public SovereignDatasetDescription getDescription() {
    testIsActive();
    return this.dataset.getDescription();
  }

  @Override
  public String getAlias() {
    testIsActive();
    return this.dataset.getAlias();
  }

  @Override
  public DatasetSchema getSchema() {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public SovereignStorage<?, ?> getStorage() {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public SovereignExtinctionException getExtinctionException() {
    throw new UnsupportedOperationException("RawDataset does not support this operation");
  }

  @Override
  public long getAllocatedHeapStorageSize() {
    return dataset.getAllocatedHeapStorageSize();
  }

  @Override
  public long getOccupiedHeapStorageSize() {
    return dataset.getOccupiedHeapStorageSize();
  }

  @Override
  public long getAllocatedPrimaryKeyStorageSize() {
    return dataset.getAllocatedPrimaryKeyStorageSize();
  }

  @Override
  public long getOccupiedPrimaryKeyStorageSize() {
    return dataset.getOccupiedPrimaryKeyStorageSize();
  }

  @Override
  public long getPersistentBytesUsed() {
    return dataset.getPersistentBytesUsed();
  }

  @Override
  public long getAllocatedPersistentSupportStorage() {
    return dataset.getAllocatedPersistentSupportStorage();
  }

  @Override
  public long getOccupiedPersistentSupportStorage() {
    return dataset.getOccupiedPersistentSupportStorage();
  }

  @Override
  public long getAllocatedIndexStorageSize() {
    return dataset.getAllocatedIndexStorageSize();
  }

  @Override
  public long recordCount() {
    return dataset.recordCount();
  }
}
