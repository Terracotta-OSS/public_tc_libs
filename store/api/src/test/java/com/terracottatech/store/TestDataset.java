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

package com.terracottatech.store;

import com.terracottatech.store.async.AsyncDatasetReader;
import com.terracottatech.store.async.AsyncDatasetWriterReader;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.store.stream.RecordStream;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class TestDataset<K extends Comparable<K>> implements Dataset<K> {

  @Override
  public DatasetReader<K> reader() {
    return new TestDatasetReader<>();
  }

  @Override
  public DatasetWriterReader<K> writerReader() {
    return new TestDatasetWriterReader<>();
  }

  @Override
  public Indexing getIndexing() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
  }

  private static class TestDatasetReader<K extends Comparable<K>> implements DatasetReader<K> {

    @Override
    public Type<K> getKeyType() {
      return null;
    }

    @Override
    public Optional<Record<K>> get(K key) {
      return Optional.empty();
    }

    @Override
    public ReadRecordAccessor<K> on(K key) {
      return new TestReadRecordAccessor<>();
    }

    @Override
    public RecordStream<K> records() {
      return null;
    }

    @Override
    public AsyncDatasetReader<K> async() {
      return null;
    }

    @Override
    public void registerChangeListener(ChangeListener<K> listener) {
    }

    @Override
    public void deregisterChangeListener(ChangeListener<K> listener) {
    }
  }

  private static class TestDatasetWriterReader<K extends Comparable<K>> extends TestDatasetReader<K> implements DatasetWriterReader<K> {
    @Override
    public boolean add(K key, Iterable<Cell<?>> cells) {
      return false;
    }

    @Override
    public boolean update(K key, UpdateOperation<? super K> transform) {
      return false;
    }

    @Override
    public boolean delete(K key) {
      return false;
    }

    @Override
    public ReadWriteRecordAccessor<K> on(K key) {
      return new TestReadWriteRecordAccessor<>();
    }

    @Override
    public MutableRecordStream<K> records() {
      throw new UnsupportedOperationException("TestDatasetWriterReader.records not implemented");
    }

    @Override
    public AsyncDatasetWriterReader<K> async() {
      return null;
    }
  }

  private static class TestReadRecordAccessor<K extends Comparable<K>> implements ReadRecordAccessor<K> {

    @Override
    public ConditionalReadRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
      return new TestConditionalReadRecordAccessor<>();
    }

    @Override
    public <T> Optional<T> read(Function<? super Record<K>, T> mapper) {
      return Optional.empty();
    }
  }

  private static class TestReadWriteRecordAccessor<K extends Comparable<K>> implements ReadWriteRecordAccessor<K> {

    @Override
    public ConditionalReadWriteRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
      return new TestConditionalReadWriteRecordAccessor<>();
    }

    @Override
    public <T> Optional<T> read(Function<? super Record<K>, T> mapper) {
      return Optional.empty();
    }

    @Override
    public Optional<Record<K>> add(Iterable<Cell<?>> cells) {
      return Optional.empty();
    }

    @Override
    public Optional<Tuple<Record<K>, Record<K>>> update(UpdateOperation<? super K> transform) {
      return Optional.empty();
    }

    @Override
    public Optional<Record<K>> delete() {
      return Optional.empty();
    }

  }

  private static class TestConditionalReadRecordAccessor<K extends Comparable<K>> implements ConditionalReadRecordAccessor<K> {

    @Override
    public Optional<Record<K>> read() {
      return Optional.empty();
    }
  }

  private static class TestConditionalReadWriteRecordAccessor<K extends Comparable<K>> implements ConditionalReadWriteRecordAccessor<K> {

    @Override
    public Optional<Tuple<Record<K>, Record<K>>> update(UpdateOperation<? super K> transform) {
      return Optional.empty();
    }

    @Override
    public Optional<Record<K>> delete() {
      return Optional.empty();
    }

    @Override
    public Optional<Record<K>> read() {
      return Optional.empty();
    }
  }
}
