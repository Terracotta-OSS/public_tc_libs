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
package com.terracottatech.store.transactions.api;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.transactions.exception.StoreTransactionRuntimeException;
import com.terracottatech.store.transactions.exception.StoreTransactionTimeoutException;
import com.terracottatech.store.transactions.impl.TransactionControllerImpl;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This interface defines the methods that allows read and write operations on a set of {@link Dataset}s to be executed under
 * a transaction.
 *
 * A {@code Dataset} named as SystemTransactions would be created to store metadata of active transactions in the system.
 * Every transaction would have a timeout defined in seconds after which a transaction could get rolled back.
 */
public interface TransactionController extends AutoCloseable {

  Duration DEFAULT_TRANSACTION_TIMEOUT = Duration.of(15, ChronoUnit.SECONDS);

  /**
   * Execute a read-only transactional action against the given reader.
   *
   * @param reader dataset reader to be acted upon
   * @param action transactional action to perform
   * @param <T> result type
   * @param <K> dataset key type
   * @return the result of the transactional action
   * @throws Exception as thrown by the supplied action
   */
  @SuppressWarnings({"unchecked", "overloads"})
  default <T, K extends Comparable<K>> T execute(
          DatasetReader<K> reader,
          TransactionalAction<DatasetReader<K>, T> action) throws Exception {
    return transact().using("a", reader).execute(readers -> {
      return action.perform((DatasetReader<K>) readers.get("a"));
    });
  }

  /**
   * Execute a transactional action against the given writer-reader.
   *
   * @param writerReader dataset writer-reader to be acted upon
   * @param action transactional action to perform
   * @param <T> result type
   * @param <K> dataset key type
   * @return the result of the transactional action
   * @throws Exception as thrown by the supplied action
   */
  @SuppressWarnings({"unchecked", "overloads"})
  default <T, K extends Comparable<K>> T execute(
          DatasetWriterReader<K> writerReader,
          TransactionalAction<DatasetWriterReader<K>, T> action) throws Exception {
    return transact().using("a", writerReader).execute((writers, readers) -> {
      return action.perform((DatasetWriterReader<K>) writers.get("a"));
    });
  }

  /**
   * Execute a transactional task against the given writer-reader.
   *
   * @param writerReader dataset writer-reader to be acted upon
   * @param task transactional task to perform
   * @param <K> dataset key type
   * @throws Exception as thrown by the supplied action
   */
  @SuppressWarnings("unchecked")
  default <K extends Comparable<K>> void execute(
          DatasetWriterReader<K> writerReader,
          TransactionalTask<DatasetWriterReader<K>> task) throws Exception {
    transact().using("a", writerReader).execute((writers, readers) -> {
      task.execute((DatasetWriterReader<K>) writers.get("a"));
      return null;
    });
  }

  /**
   * Execute a read-only transactional action against the given pair of readers.
   *
   * @param readerA first dataset reader to be acted upon
   * @param readerB second dataset reader to be acted upon
   * @param action transactional action to perform
   * @param <T> result type
   * @param <KA> first dataset key type
   * @param <KB> second dataset key type
   * @return the result of the transactional action
   * @throws Exception as thrown by the supplied action
   */
  @SuppressWarnings({"unchecked", "overloads"})
  default <T, KA extends Comparable<KA>, KB extends Comparable<KB>> T execute(
          DatasetReader<KA> readerA,
          DatasetReader<KB> readerB,
          TransactionalBiAction<DatasetReader<KA>, DatasetReader<KB>, T> action) throws Exception {
    return transact().using("a", readerA).using("b", readerB).execute(readers -> {
      return action.perform((DatasetReader<KA>) readers.get("a"), (DatasetReader<KB>) readers.get("b"));
    });
  }

  /**
   * Execute a transactional action against the given pair of writer-readers.
   *
   * @param writerReaderA first dataset writer-reader to be acted upon
   * @param writerReaderB second dataset writer-reader to be acted upon
   * @param action transactional action to perform
   * @param <T> result type
   * @param <KA> first dataset key type
   * @param <KB> second dataset key type
   * @return the result of the transactional action
   * @throws Exception as thrown by the supplied action
   */
  @SuppressWarnings({"unchecked", "overloads"})
  default <T, KA extends Comparable<KA>, KB extends Comparable<KB>> T execute(
          DatasetWriterReader<KA> writerReaderA,
          DatasetWriterReader<KB> writerReaderB,
          TransactionalBiAction<DatasetWriterReader<KA>, DatasetWriterReader<KB>, T> action) throws Exception {
    return transact().using("a", writerReaderA).using("b", writerReaderB).execute((writers, readers) -> {
      return action.perform((DatasetWriterReader<KA>) writers.get("a"), (DatasetWriterReader<KB>) writers.get("b"));
    });
  }

  /**
   * Execute a transactional task against the given pair of writer-readers.
   *
   * @param writerReaderA first dataset writer-reader to be acted upon
   * @param writerReaderB second dataset writer-reader to be acted upon
   * @param task transactional task to perform
   * @param <KA> first dataset key type
   * @param <KB> second dataset key type
   * @throws Exception as thrown by the supplied action
   */
  @SuppressWarnings("unchecked")
  default <KA extends Comparable<KA>, KB extends Comparable<KB>> void execute(
          DatasetWriterReader<KA> writerReaderA,
          DatasetWriterReader<KB> writerReaderB,
          TransactionalBiTask<DatasetWriterReader<KA>, DatasetWriterReader<KB>> task) throws Exception {
    transact().using("a", writerReaderA).using("b", writerReaderB).execute((writers, readers) -> {
      task.execute((DatasetWriterReader<KA>) writers.get("a"), (DatasetWriterReader<KB>) writers.get("b"));
      return null;
    });
  }

  /**
   * Creates a {@link ReadOnlyExecutionBuilder} for a transaction having the timeout value as the default timeout.
   *
   * @return a {@code ReadOnlyExecutionBuilder} that is used to execute transactions.
   */
  ReadOnlyExecutionBuilder transact();

  /**
   * Creates a {@link TransactionController} object to execute transactions on the {@link Dataset}s managed by the
   * given {@link DatasetManager}. It also creates the SystemTransactions dataset with the given {@link DatasetConfiguration}.
   * <p>
   * Note that if the SystemTransactions {@code Dataset} already exists then nothing is done in that regards. The given
   * {@link DatasetConfiguration} is not checked for compatibility.
   *
   * @param datasetManager The instance of {@code DatasetManager} whose {@code Dataset}s would participate in a
   *                       transaction.
   * @param systemTransactionsDatasetConfigurationBuilder The {@code DatasetConfigurationBuilder} for SystemTransactions {@code Dataset}.
   * @return {@code TransactionController} instance that could execute transactions on {@code Dataset}s managed by the
   * given {@code DatasetManager}.
   * @throws StoreException if it fails to create the SystemTransactions {@code Dataset}.
   */
  static TransactionController createTransactionController(DatasetManager datasetManager,
                                                           DatasetConfigurationBuilder systemTransactionsDatasetConfigurationBuilder) throws StoreException {
    return new TransactionControllerImpl(datasetManager, systemTransactionsDatasetConfigurationBuilder);
  }

  /**
   * Creates a {@link TransactionController} object to execute transactions on the {@link Dataset}s managed by the given
   * {@link DatasetManager}.
   * <p>
   * Note that a {@code StoreException} is thrown if the SystemTransactions {@code Dataset} does
   * not already exist.
   *
   * @param datasetManager The instance of {@code DatasetManager} whose {@code Dataset}s would participate in a
   *                       transaction.
   * @return {@code TransactionController} instance that could execute transactions on {@code Dataset}s managed by the
   * given {@code DatasetManager}.
   * @throws StoreException if the SystemTransactions {@code Dataset} does not already exist.
   */
  static TransactionController  createTransactionController(DatasetManager datasetManager) throws StoreException {
    return new TransactionControllerImpl(datasetManager);
  }

  /**
   * Creates a {@link TransactionController} object with the given default transaction timeout.
   *
   * @param timeOut default timeout of a transaction
   * @param timeUnit the time unit of the time argument
   * @return the instance of {@code TransactionController} with the given default transaction timeout
   */
  TransactionController withDefaultTimeOut(long timeOut, TimeUnit timeUnit);

  /**
   * This interface is used to add {@code DatasetReader}s and {@code DatasetWriter}s participating in a transaction.
   */
  interface ExecutionBuilder {

    /**
     * Set the timeout for the transaction to be executed
     * @param timeOut timeout if the transaction
     * @param timeUnit the time unit of the time argument
     * @return instance of {@code ExecutionBuilder} with the given transaction timeout
     */
    ExecutionBuilder timeout(long timeOut, TimeUnit timeUnit);

    /**
     * Adds a {@link DatasetWriterReader} as a participant in the transaction
     * @param name name that maps to the given {@code DatasetWriterReader}
     * @param writerReader participating {@code DatasetWriterReader}
     * @return instance of {@code ExecutionBuilder} with the given {@code DatasetWriterReader} added as participant
     */
    ExecutionBuilder using(String name, DatasetWriterReader<?> writerReader);

    /**
     * Adds a {@link DatasetReader} as a participant in the transaction
     * @param name name that maps to the given {@code DatasetReader}
     * @param reader participating {@code DatasetReader}
     * @return instance of {@code ExecutionBuilder} with the given {@code DatasetReader} added as participant
     */
    ExecutionBuilder using(String name, DatasetReader<?> reader);
  }

  /**
   * This interface is used to add {@code DatasetReader}s and {@code DatasetWriter}s participating in a transaction and
   * then execute a given read only transaction on these participants.
   */
  interface ReadOnlyExecutionBuilder extends ExecutionBuilder {

    @Override
    ReadOnlyExecutionBuilder timeout(long timeOut, TimeUnit timeUnit);

    @Override
    ReadWriteExecutionBuilder using(String name, DatasetWriterReader<?> writerReader);

    @Override
    ReadOnlyExecutionBuilder using(String name, DatasetReader<?> reader);

    /**
     * Executes the given transaction using the added participants
     * @param action transactional action to be performed
     * @param <T> return type of the transaction argument
     * @return the transaction result
     * @throws Exception if the transaction could not be completed
     * @throws StoreTransactionTimeoutException if the transaction timed out before completion
     */
    <T> T execute(TransactionalAction<Map<String, DatasetReader<?>>, T> action) throws Exception;

    /**
     * Begins a new transaction using the configured participants.
     *
     * @return a handle to the new transaction
     * @deprecated in favor of {@link #execute(TransactionalAction)}
     */
    @Deprecated
    ReadOnlyTransaction begin();
  }

  /**
   * This interface is used to add {@code DatasetReader}s and {@code DatasetWriter}s participating in a transaction and
   * then execute a given read write transaction on these participants.
   */
  interface ReadWriteExecutionBuilder extends ExecutionBuilder {

    @Override
    ReadWriteExecutionBuilder timeout(long timeOut, TimeUnit timeUnit);

    @Override
    ReadWriteExecutionBuilder using(String name, DatasetReader<?> reader);

    @Override
    ReadWriteExecutionBuilder using(String name, DatasetWriterReader<?> writerReader);

    /**
     * Executes the given transaction using the added participants
     * @param action transactional action to be performed
     * @param <T> return type of the transaction argument
     * @return the transaction result
     * @throws Exception if the transaction could not be completed, internal rollback would be triggered to cleanup
     * unfinished transactional work.
     * @throws StoreTransactionTimeoutException if the transaction timed out before completion, internal rollback would
     * be triggered to cleanup unfinished transactional work.
     * Note that if the internal rollback also fails with some exception, that exception would be added as suppressed to
     * the thrown exception.
     */
    <T> T execute(TransactionalBiAction<Map<String, DatasetWriterReader<?>>, Map<String, DatasetReader<?>>, T> action) throws Exception;

    /**
     * Begins a new transaction using the configured participants.
     *
     * @return a handle to the new transaction
     * @deprecated in favor of {@link #execute(TransactionalBiAction)}
     */
    @Deprecated
    ReadWriteTransaction begin();
  }

  /**
   * Direct representation of a transaction to support external transaction control.
   * <p>
   *   This form of transactional execution is strongly discouraged in favor of the various lambda injection forms.
   * </p>
   */
  interface Transaction {

    /**
     * Commit this transaction.
     *
     * @throws StoreTransactionTimeoutException if the transaction has already timed out. This would also lead to an
     * internal rollback of the transaction.
     * @throws StoreTransactionRuntimeException if the internal rollback triggered by the transaction time out itself
     * fails or if the transaction has already ended.
     */
    void commit();

    /**
     * Rollback this transaction.
     *
     * @throws StoreTransactionRuntimeException if the transaction has already ended.
     */
    void rollback();
  }

  /**
   * Direct reference to a read-only transaction.
   */
  interface ReadOnlyTransaction extends Transaction {

    /**
     * Retrieve the reader with the given resource name.
     *
     * @param name reader name
     * @param <K> expected key type
     * @return a transactional dataset reader
     */
    <K extends Comparable<K>> DatasetReader<K> reader(String name);
  }

  /**
   * Direct reference to a read-write transaction.
   */
  interface ReadWriteTransaction extends ReadOnlyTransaction {

    /**
     * Retrieve the writer/reader with the given resource name.
     *
     * @param name writer/reader name
     * @param <K> expected key type
     * @return a transactional dataset writer/reader
     */
    <K extends Comparable<K>> DatasetWriterReader<K> writerReader(String name);
  }

  /**
   * Closes this and the underlying resources.
   * <p>
   * Note that starting or executing a new {@code Transaction} using a closed {@code TransactionController} instance
   * would result into a {@code StoreTransactionRuntimeException}.
   * Similarly, any operation performed using the active transaction instances that were created using this
   * {@code TransactionController} instance would result into a {@code StoreTransactionRuntimeException}.
   */
  @Override
  void close();
}
