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
package com.terracottatech.store.wrapper;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.store.Cell;
import com.terracottatech.store.ConditionalReadWriteRecordAccessor;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.async.AsyncDatasetWriterReader;
import com.terracottatech.store.async.ExecutorDrivenAsyncDatasetWriterReader;
import com.terracottatech.store.stream.MutableRecordStream;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

import static java.util.Optional.ofNullable;

/**
 *
 * @author cdennis
 */
public class WrapperDatasetWriterReader<K extends Comparable<K>> extends WrapperDatasetReader<K> implements DatasetWriterReader<K> {

  private final SovereignDataset.Durability durability;
  private final Executor asyncExecutor;

  public WrapperDatasetWriterReader(SovereignDataset<K> backing, Executor asyncExecutor, SovereignDataset.Durability durability) {
    super(backing, asyncExecutor);
    this.durability = durability;
    this.asyncExecutor = asyncExecutor;
  }

  @Override
  public boolean add(K key, Iterable<Cell<?>> cells) {
    return getSovereignBacking().add(durability, key, cells) == null;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public boolean update(K key, UpdateOperation<? super K> transform) {
    //TODO fix this nasty raw type hack
    return getSovereignBacking().applyMutation(durability, key,
                                               r -> true,
                                               (Record r) -> transform.apply(r), (a, b) -> Boolean.TRUE).isPresent();
  }

  @Override
  public boolean delete(K key) {
    return getSovereignBacking().delete(durability, key) != null;
  }

  @Override
  public ReadWriteRecordAccessor<K> on(K key) {
    return new ReadWriteRecordAccessorImpl(key);
  }

  @Override
  public MutableRecordStream<K> records() {
    return new WrapperMutableRecordStream<>(getSovereignBacking().records(), getSovereignBacking(), durability);
  }
  @Override
  public AsyncDatasetWriterReader<K> async() {
    return new ExecutorDrivenAsyncDatasetWriterReader<>(this, asyncExecutor);
  }

  private class ReadWriteRecordAccessorImpl extends ReadRecordAccessorImpl implements ReadWriteRecordAccessor<K> {

    private final K key;

    public ReadWriteRecordAccessorImpl(K key) {
      super(key);
      this.key = key;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public ConditionalReadWriteRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
      return new ConditionalReadWriteRecordAccessor<K>() {
        @Override
        public Optional<Tuple<Record<K>, Record<K>>> update(UpdateOperation<? super K> transform) {
          return getSovereignBacking().applyMutation(durability, key, predicate, (Record r) -> {
            return transform.apply(r);
          }, (Record<K> a, Record<K> b) -> {
            return Optional.of(Tuple.of(a, b));
          }).flatMap(r -> r);
        }

        @Override
        public Optional<Record<K>> delete() {
          return ofNullable(getSovereignBacking().delete(durability, key, predicate));
        }

        @Override
        public Optional<Record<K>> read() {
          return get(key).filter(predicate);
        }
      };
    }

    @Override
    public Optional<Record<K>> add(Iterable<Cell<?>> cells) {
      return ofNullable(getSovereignBacking().add(durability, key, cells));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Optional<Tuple<Record<K>, Record<K>>> update(UpdateOperation<? super K> transform) {
      return getSovereignBacking().applyMutation(durability, key,
                                                 r -> true,
                                                 (Record r) -> transform.apply(r), Tuple::of);
    }

    @Override
    public Optional<Record<K>> delete() {
      return ofNullable(getSovereignBacking().delete(durability, key));
    }
  }

}
