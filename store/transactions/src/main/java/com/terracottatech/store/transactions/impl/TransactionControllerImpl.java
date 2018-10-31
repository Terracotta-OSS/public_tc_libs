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
package com.terracottatech.store.transactions.impl;

import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.transactions.api.TransactionController;
import com.terracottatech.store.transactions.api.TransactionalAction;
import com.terracottatech.store.transactions.api.TransactionalBiAction;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class TransactionControllerImpl implements TransactionController {

  private final DatasetManager datasetManager;
  private final SystemTransactions systemTransactions;
  private final Duration defaultTransactionTimeOut;

  public TransactionControllerImpl(DatasetManager datasetManager) throws StoreException {
    this(datasetManager, new SystemTransactions(datasetManager), DEFAULT_TRANSACTION_TIMEOUT);
  }

  private TransactionControllerImpl(DatasetManager datasetManager, SystemTransactions systemTransactions, Duration timeout) {
    this.datasetManager = datasetManager;
    this.systemTransactions = systemTransactions;
    defaultTransactionTimeOut = timeout;
  }

  public TransactionControllerImpl(DatasetManager datasetManager,
                                   DatasetConfigurationBuilder systemTransactionsDatasetConfigurationBuilder) throws StoreException {
    this(datasetManager,
      new SystemTransactions(datasetManager, systemTransactionsDatasetConfigurationBuilder),
      DEFAULT_TRANSACTION_TIMEOUT);
  }

  @Override
  public ReadOnlyExecutionBuilder transact() {
    return new ReadOnlyExecutionBuilderImpl();
  }

  @Override
  public TransactionController withDefaultTimeOut(long timeOut, TimeUnit timeUnit) {
    Duration timeOutDuration = Duration.of(timeOut, toChronoUnit(timeUnit));
    if (timeOutDuration.isNegative()) {
      throw new IllegalArgumentException("timeOut parameter value cannot be negative.");
    }
    return new TransactionControllerImpl(datasetManager, systemTransactions, timeOutDuration);
  }

  abstract class AbstractExecutionBuilder implements ExecutionBuilder {
    protected final Duration timeout;
    protected final Map<String, DatasetReader<?>> readers;

    AbstractExecutionBuilder(Duration timeout, Map<String, DatasetReader<?>> readers) {
      this.timeout = timeout;
      this.readers = readers;
    }
  }

  class ReadOnlyExecutionBuilderImpl extends AbstractExecutionBuilder implements ReadOnlyExecutionBuilder {

    ReadOnlyExecutionBuilderImpl() {
      super(defaultTransactionTimeOut, emptyMap());
    }

    private ReadOnlyExecutionBuilderImpl(Duration timeout, Map<String, DatasetReader<?>> readers) {
      super(timeout, readers);
    }

    @Override
    public ReadOnlyExecutionBuilderImpl timeout(long timeOut, TimeUnit timeUnit) {
      Duration timeOutDuration = Duration.of(timeOut, toChronoUnit(timeUnit));
      if (timeOutDuration.isNegative()) {
        throw new IllegalArgumentException("timeOut parameter value cannot be negative.");
      }
      return new ReadOnlyExecutionBuilderImpl(timeOutDuration, readers);
    }

    @Override
    public ReadWriteExecutionBuilderImpl using(String name, DatasetWriterReader<?> writerReader) {
      requireNonNull(name);
      requireNonNull(writerReader);
      Map<String, DatasetWriterReader<?>> writerReaders = new HashMap<>();
      writerReaders.put(name, writerReader);
      return new ReadWriteExecutionBuilderImpl(timeout, readers, writerReaders);
    }

    @Override
    public ReadOnlyExecutionBuilderImpl using(String name, DatasetReader<?> reader) {
      requireNonNull(name);
      requireNonNull(reader);
      Map<String, DatasetReader<?>> newReaders = new HashMap<>(readers);
      newReaders.compute(name, (key, old) -> {
        if (old != null) {
          throw new IllegalStateException("A DatasetReader with the given name " + name + " has already been added.");
        } else {
          return reader;
        }
      });
      return new ReadOnlyExecutionBuilderImpl(timeout, newReaders);
    }

    @Override
    public <T> T execute(TransactionalAction<Map<String, DatasetReader<?>>, T> action) throws Exception {
      requireNonNull(action);
      ReadOnlyTransactionImpl<T> txn =
        new ReadOnlyTransactionImpl<>(systemTransactions, timeout, readers,
          transactionalReaders -> action.perform(readers(transactionalReaders)));
      return txn.execute();
    }

    @Deprecated
    @Override
    public ReadOnlyTransaction begin() {
      return new ReadOnlyTransactionImpl<>(systemTransactions, timeout, readers, null);
    }
  }

  class ReadWriteExecutionBuilderImpl extends AbstractExecutionBuilder implements ReadWriteExecutionBuilder {

    private final Map<String, DatasetWriterReader<?>> writerReaders;

    ReadWriteExecutionBuilderImpl(Duration timeout, Map<String, DatasetReader<?>> readers,
                                         Map<String, DatasetWriterReader<?>> writerReaders) {
      super(timeout, readers);
      this.writerReaders = writerReaders;
    }

    @Override
    public ReadWriteExecutionBuilderImpl timeout(long timeOut, TimeUnit timeUnit) {
      Duration timeOutDuration = Duration.of(timeOut, toChronoUnit(timeUnit));
      if (timeOutDuration.isNegative()) {
        throw new IllegalArgumentException("timeOut parameter value cannot be negative.");
      }
      return new ReadWriteExecutionBuilderImpl(timeOutDuration, readers, writerReaders);
    }

    @Override
    public ReadWriteExecutionBuilderImpl using(String name, DatasetWriterReader<?> writerReader) {
      requireNonNull(name);
      requireNonNull(writerReader);
      Map<String, DatasetWriterReader<?>> newWriterReaders = new HashMap<>(writerReaders);
      newWriterReaders.compute(name, (key, old) -> {
        if (old != null) {
          throw new IllegalStateException("A DatasetWriterReader with the given name " + name + " has already been added.");
        } else {
          return writerReader;
        }
      });
      return new ReadWriteExecutionBuilderImpl(timeout, readers, newWriterReaders);
    }

    @Override
    public ReadWriteExecutionBuilderImpl using(String name, DatasetReader<?> reader) {
      requireNonNull(name);
      requireNonNull(reader);
      Map<String, DatasetReader<?>> newReaders = new HashMap<>(readers);
      newReaders.compute(name, (key, old) -> {
        if (old != null) {
          throw new IllegalStateException("A DatasetReader with the given name " + name + " has already been added.");
        } else {
          return reader;
        }
      });
      return new ReadWriteExecutionBuilderImpl(timeout, newReaders, writerReaders);
    }

    @Override
    public <T> T execute(TransactionalBiAction<Map<String, DatasetWriterReader<?>>, Map<String, DatasetReader<?>>, T> action) throws Exception {
      requireNonNull(action);
      ReadWriteTransactionImpl<T> txn = new ReadWriteTransactionImpl<>(systemTransactions, timeout, readers, writerReaders,
        (transactionalWriterReaders, transactionalReaders) ->
          action.perform(writerReaders(transactionalWriterReaders), readers(transactionalReaders)));
      return txn.execute();
    }

    @Deprecated
    @Override
    public ReadWriteTransaction begin() {
      return new ReadWriteTransactionImpl<>(systemTransactions, timeout, readers, writerReaders, null);
    }
  }

  @Override
  public void close() {
    systemTransactions.close();
  }

  private static ChronoUnit toChronoUnit(TimeUnit timeUnit) {
    if(timeUnit == null) {
      return null;
    }

    switch (timeUnit) {
      case NANOSECONDS:  return ChronoUnit.NANOS;
      case MICROSECONDS: return ChronoUnit.MICROS;
      case MILLISECONDS: return ChronoUnit.MILLIS;
      case SECONDS:      return ChronoUnit.SECONDS;
      case MINUTES:      return ChronoUnit.MINUTES;
      case HOURS:        return ChronoUnit.HOURS;
      case DAYS:         return ChronoUnit.DAYS;
      default: throw new AssertionError();
    }
  }

  private static Map<String, DatasetReader<?>> readers(Map<String, TransactionalDatasetReader<?>> readers) {
    return readers.entrySet().stream()
      .collect(toMap(HashMap.Entry::getKey,
        entry -> (DatasetReader<?>) (entry.getValue()),
        (entry1, entry2) -> {throw new AssertionError("Duplicate Entry for a key found"); }
      ));
  }

  private static Map<String, DatasetWriterReader<?>> writerReaders(Map<String, TransactionalDatasetWriterReader<?>> writerReaders) {
    return writerReaders.entrySet().stream()
      .collect(toMap(HashMap.Entry::getKey,
        entry -> (DatasetWriterReader<?>) (entry.getValue()),
        (entry1, entry2) -> {throw new AssertionError("Duplicate Entry for a key found"); }
      ));
  }
}
