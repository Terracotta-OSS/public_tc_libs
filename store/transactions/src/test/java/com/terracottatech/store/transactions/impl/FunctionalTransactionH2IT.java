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
import com.terracottatech.store.Type;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.transactions.api.TransactionController;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static com.terracottatech.store.Record.keyFunction;
import static com.terracottatech.store.UpdateOperation.write;
import static com.terracottatech.store.common.InterruptHelper.getUninterruptibly;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * This test is the functional API equivalent to the {@link ExternalTransactionControlH2IT} test.  For a description of
 * the test cases used here go look at that class.
 * <p>
 * The lock step execution in this class is governed by a synchronous queue that forms a bridge between the thread
 * running the transaction that is spawned when the transaction is started and the test thread itself.  The test thread
 * feeds transaction operations (and the subsequent assertions) to the synchronous task where they are picked up by the
 * executor thread and run.
 */
public class FunctionalTransactionH2IT extends AbstractH2IT {

  private static final Collection<Predicate<List<Operation>>> DIVERGENT_PATTERNS = asList(
      //add delete releases 'lock' in TC
      partialOrdering(add("txn1", "dataset1", 1, 1), delete("txn1", "dataset1", 1), add("txn2", "dataset1", 1, 2), commit("txn1")),
      partialOrdering(add("txn1", "dataset1", 1, 1), delete("txn1", "dataset1", 1), add("txn2", "dataset1", 1, 2), rollback("txn1")),

      //lock timeout == transaction timeout in TC
      partialOrdering(add("txn1", "dataset1", 1, 1), add("txn2", "dataset1", 1, 2), commit("txn1")),
      partialOrdering(add("txn2", "dataset1", 1, 2), add("txn1", "dataset1", 1, 1), commit("txn2")),
      partialOrdering(add("txn1", "dataset1", 1, 1), add("txn2", "dataset1", 1, 2), rollback("txn1")),
      partialOrdering(add("txn2", "dataset1", 1, 2), add("txn1", "dataset1", 1, 1), rollback("txn2"))
  );

  @Test
  public void testSingleTransactionSingleResource() {
    legalSequences(new HashSet<>(asList(
        begin("txn1", 60, SECONDS),
        commit("txn1"),
        rollback("txn1"),
        add("txn1", "dataset1", 1, 1),
        update("txn1", "dataset1", 1, 2),
        update("txn1", "dataset1", 1, 3),
        delete("txn1", "dataset1", 1)
    ))).parallel().forEach(parallelism(4, this::execute));
  }

  @Test
  public void testTwoTransactionSingleResource() {
    legalSequences(new HashSet<>(asList(
        begin("txn1", 60, SECONDS),
        commit("txn1"),
        rollback("txn1"),
        add("txn1", "dataset1", 1, 1),
        delete("txn1", "dataset1", 1),

        begin("txn2", 60, SECONDS),
        commit("txn2"),
        rollback("txn2"),
        add("txn2", "dataset1", 1, 2),
        delete("txn2", "dataset1", 1)
    ))).filter(sequence -> !DIVERGENT_PATTERNS.stream().anyMatch(p -> p.test(sequence)))
        .parallel().forEach(parallelism(4, this::execute));
  }

  @Test
  public void testSingleTransactionTwoResource() {
    legalSequences(new HashSet<>(asList(
        begin("txn1", 60, SECONDS),
        commit("txn1"),
        rollback("txn1"),
        add("txn1", "dataset1", 1, 1),
        delete("txn1", "dataset1", 1),

        add("txn1", "dataset2", 1, 1),
        delete("txn1", "dataset2", 1)
    ))).parallel().forEach(parallelism(4, this::execute));
  }

  private static Operation begin(String txn, long timeout, TimeUnit unit) {
    return new TransactionBegin(txn) {
      @Override
      UnaryOperator<Object> tcTransition() {
        return state -> {
          DatasetManager datasetManager = (DatasetManager) state;
          try {
            TransactionController.ExecutionBuilder builder = TransactionController.createTransactionController(datasetManager,
                datasetManager.datasetConfiguration().offheap("offheap")).withDefaultTimeOut(timeout, unit).transact();
            for (String dataset : datasetManager.listDatasets().keySet()) {
              if (!dataset.equals("SystemTransactions")) {
                builder = builder.using(dataset, datasetManager.getDataset(dataset, Type.INT).writerReader());
              }
            }

            return new TransactionState((TransactionController.ReadWriteExecutionBuilder) builder);
          } catch (StoreException e) {
            throw new AssertionError(e);
          }
        };
      }
    };
  }

  private static Operation commit(String txn) {
    return new TransactionCommit(txn) {
      @Override
      public UnaryOperator<Object> tcTransition() {
        return state -> ((TransactionState) state).commit();
      }
    };
  }

  private static Operation rollback(String txn) {
    return new TransactionRollback(txn) {
      @Override
      public UnaryOperator<Object> tcTransition() {
        return state -> ((TransactionState) state).rollback();
      }
    };
  }

  private static Operation add(String transaction, String dataset, int key, int cell) {
    return new FunctionalTransactionOperation(transaction, dataset,
        format("INSERT INTO %s VALUES (%d, %d)", dataset, key, cell), ds -> ds.add(key, CELL.newCell(cell))) {
      @Override
      public String toString() {
        return "add(\"" + transaction + "\", \"" + dataset + "\", " + key + ", " + cell + ")";
      }
    };
  }

  private static Operation update(String transaction, String dataset, int key, int cell) {
    return new FunctionalTransactionOperation(transaction, dataset,
        format("UPDATE %s SET cell = %d WHERE key = %d", dataset, cell, key), ds -> ds.update(key, write(CELL).value(cell))) {
      @Override
      public String toString() {
        return "update(\"" + transaction + "\", \"" + dataset + "\", " + key + ", " + cell + ")";
      }
    };
  }

  private static Operation delete(String transaction, String dataset, int key) {
    return new FunctionalTransactionOperation(transaction, dataset,
        format("DELETE FROM %s WHERE key = %d", dataset, key), ds -> ds.delete(key)){
      @Override
      public String toString() {
        return "delete(\"" + transaction + "\", \"" + dataset + "\", " + key + ")";
      }
    };
  }

  static class FunctionalTransactionOperation extends TransactionOperation {

    private final Consumer<DatasetWriterReader<Integer>> tcAction;

    FunctionalTransactionOperation(String txn, String dataset, String sql, Consumer<DatasetWriterReader<Integer>> tcAction) {
      super(txn, dataset, sql);
      this.tcAction = tcAction;
    }

    @Override
    @SuppressWarnings("unchecked")
    UnaryOperator<Object> tcTransition() {
      return (Object state) -> ((TransactionState) state).execute(getDataset(), tcAction);
    }
  }

  @SuppressWarnings("try")
  private void execute(List<Operation> sequence) {
    Collection<String> tables = tablesIn(sequence);
    Collection<String> transactions = transactionsIn(sequence);
    String db = UUID.randomUUID().toString();
    try (Closeable database = initializeDatabase(db, tables); DatasetManager datasetManager = initializeTcStore(tables)) {
      Map<String, Object> dbState = initializeDatabaseState(db, transactions);
      Map<String, Object> tcState = initializeTcStoreState(datasetManager, transactions);
      for (Operation op : sequence) {
        boolean tcResult = op.tcExecute(tcState);
        boolean dbResult = op.dbExecute(dbState);
        assertThat(op + " in " + sequence.toString(), tcResult, is(dbResult));
        for (String txn : transactions) {
          Object dbTransaction = dbState.get(txn);
          if (dbTransaction instanceof Connection) {
            TransactionState tcTransaction = (TransactionState) tcState.get(txn);
            assertThat("From " + txn + " after " + op + " in " + sequence.toString(), tcTransaction.state(tables), is(dbState((Connection) dbTransaction, tables)));
          }
        }
        assertThat("From outside after " + op + " in " + sequence.toString(), tcState(datasetManager, tables), is(dbState(db, tables)));
      }
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  private static Map<String,Object> initializeTcStoreState(DatasetManager datasetManager, Collection<String> transactions) {
    Map<String, Object> state = new HashMap<>();
    for (String txn : transactions) {
      state.put(txn, datasetManager);
    }
    return state;
  }

  private static class TransactionState {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Future<?> future;

    private final BlockingQueue<Consumer<Map<String, DatasetWriterReader<?>>>> tasks = new SynchronousQueue<>();
    private volatile boolean committed = false;

    public TransactionState(TransactionController.ReadWriteExecutionBuilder builder) {
      this.future = executor.submit(() -> builder.execute((writers, readers) -> {
        while (!committed) {
          Consumer<Map<String, DatasetWriterReader<?>>> task = tasks.poll(60, TimeUnit.SECONDS);
          if (task == null) {
            throw new AssertionError("Timeout retrieving workload for executor. Is the driver dead?");
          } else {
            task.accept(writers);
          }
        }
        return null;
      }));
    }

    @SuppressWarnings("unchecked")
    public TransactionState execute(String dataset, Consumer<DatasetWriterReader<Integer>> tcAction) {
      try {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
          offer(writers -> {
            try {
              tcAction.accept((DatasetWriterReader<Integer>) writers.get(dataset));
              future.complete(null);
            } catch (Throwable t) {
              future.completeExceptionally(t);
            }
          });
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        }

        try {
          getUninterruptibly(future);
        } catch (ExecutionException e) {
          throw new AssertionError(e.getCause());
        }
        return this;
      } finally {
        checkExecutorState();
      }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Map<Integer, Optional<Integer>>> state(Collection<String> tables) {
      try {
        CompletableFuture<Map<String, Map<Integer, Optional<Integer>>>> future = new CompletableFuture<>();
        try {
          offer(writers -> {
            try {
              future.complete(tables.stream().collect(toMap(identity(), table -> ((DatasetReader<Integer>) writers.get(table)).records().collect(toMap(keyFunction(), CELL.value())))));
            } catch (Throwable t) {
              future.completeExceptionally(t);
            }
          });
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        }

        try {
          return getUninterruptibly(future);
        } catch (ExecutionException e) {
          throw new AssertionError(e);
        }
      } finally {
        checkExecutorState();
      }
    }

    public Object commit() {
      try {
        offer(writers -> {
          committed = true;
        });
        getUninterruptibly(future);
        return null;
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    public Object rollback() {
      RuntimeException rollback = new RuntimeException();
      try {
        offer(writers -> {throw rollback;});
        try {
          getUninterruptibly(future);
        } catch (ExecutionException e) {
          if (e.getCause() == rollback) {
            return null;
          }
        }
        throw new AssertionError("Expected rollback exception");
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    private void checkExecutorState() {
      if (future.isDone()) {
        try {
          future.get();
          fail("Unexpected normal termination of executor");
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        } catch (ExecutionException e) {
          throw new AssertionError(e.getCause());
        }
      }
    }

    private void offer(Consumer<Map<String, DatasetWriterReader<?>>> task) throws InterruptedException {
      if (!tasks.offer(task, 60, TimeUnit.SECONDS)) {
        checkExecutorState();
        throw new AssertionError("Timeout offering workload to executor.");
      }
    }
  }
}
