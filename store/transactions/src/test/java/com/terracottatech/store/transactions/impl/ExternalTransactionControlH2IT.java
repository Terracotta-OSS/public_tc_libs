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

import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.transactions.api.TransactionController;
import com.terracottatech.store.transactions.api.TransactionController.ReadOnlyTransaction;
import com.terracottatech.store.transactions.api.TransactionController.ReadWriteTransaction;
import org.junit.Test;

import java.io.Closeable;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static com.terracottatech.store.UpdateOperation.write;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * This test validates the behavior of the transactions layer by running equivalent operation sequences in parallel on
 * both TCStore and the H2 embedded SQL database.
 * <p>
 * The tests works by generating every legal sequence of a given set of transactional operations and then executing them
 * in lock step against the two systems.  After each step we compare:
 * <ul>
 *   <li>the result of the operations (success or failure)</li>
 *   <li>the state of two 'databases' as viewed from:
 *   <ul>
 *     <li>all open transactions</li>
 *     <li>a newly opened transaction</li>
 *     <li>direct access on to the dataset</li>
 *   </ul>
 *   </li>
 * </ul>
 * <p>
 * The legal sequences must:
 * <ul>
 *   <li>open and close all configured transactions</li>
 *   <li>access all configured resources</li>
 *   <li>honor the rules given in the {@link com.terracottatech.store.transactions.impl.AbstractH2IT.Operation#legalAfter(List)} methods</li>
 *   <li>is not one of the "divergent patterns" where we know our behavior is justifiably different from H2</li>
 * </ul>
 */
public class ExternalTransactionControlH2IT extends AbstractH2IT {

  private static final Collection<Predicate<List<Operation>>> DIVERGENT_PATTERNS = asList(
      //add then delete releases the 'lock' in TC, but does not in H2
      partialOrdering(add("txn1", "dataset1", 1, 1), delete("txn1", "dataset1", 1), add("txn2", "dataset1", 1, 2), commit("txn1")),
      partialOrdering(add("txn1", "dataset1", 1, 1), delete("txn1", "dataset1", 1), add("txn2", "dataset1", 1, 2), rollback("txn1")),

      //lock timeout == transaction timeout in TC, but not so in H2
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
      @SuppressWarnings("deprecation")
      public UnaryOperator<Object> tcTransition() {
        return state -> {
          try {
            DatasetManager datasetManager = (DatasetManager) state;
            TransactionController.ExecutionBuilder builder = TransactionController.createTransactionController(datasetManager,
                datasetManager.datasetConfiguration().offheap("offheap")).withDefaultTimeOut(timeout, unit).transact();
            for (String dataset : datasetManager.listDatasets().keySet()) {
              if (!dataset.equals("SystemTransactions")) {
                builder = builder.using(dataset, datasetManager.getDataset(dataset, Type.INT).writerReader());
              }
            }
            return ((TransactionController.ReadWriteExecutionBuilder) builder).begin();
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
        return state -> {
          if (state instanceof ReadWriteTransaction) {
            ((ReadWriteTransaction) state).commit();
            return null;
          } else if (state instanceof ReadOnlyTransaction) {
            ((ReadOnlyTransaction) state).commit();
            return null;
          } else {
            throw new AssertionError("Unexpected state " + state);
          }
        };
      }
    };
  }

  private static Operation rollback(String txn) {
    return new TransactionRollback(txn) {
      @Override
      public UnaryOperator<Object> tcTransition() {
        return state -> {
          if (state instanceof ReadWriteTransaction) {
            ((ReadWriteTransaction) state).rollback();
            return null;
          } else if (state instanceof ReadOnlyTransaction) {
            ((ReadOnlyTransaction) state).rollback();
            return null;
          } else {
            throw new AssertionError("Unexpected state " + state);
          }
        };
      }
    };
  }

  private static Operation add(String transaction, String dataset, int key, int cell) {
    return new ExternalTransactionOperation(transaction, dataset,
        format("INSERT INTO %s VALUES (%d, %d)", dataset, key, cell), ds -> ds.add(key, CELL.newCell(cell))) {
      @Override
      public String toString() {
        return "add(\"" + transaction + "\", \"" + dataset + "\", " + key + ", " + cell + ")";
      }
    };
  }

  private static Operation update(String transaction, String dataset, int key, int cell) {
    return new ExternalTransactionOperation(transaction, dataset,
        format("UPDATE %s SET cell = %d WHERE key = %d", dataset, cell, key), ds -> ds.update(key, write(CELL).value(cell))) {
      @Override
      public String toString() {
        return "update(\"" + transaction + "\", \"" + dataset + "\", " + key + ", " + cell + ")";
      }
    };
  }

  private static Operation delete(String transaction, String dataset, int key) {
    return new ExternalTransactionOperation(transaction, dataset,
        format("DELETE FROM %s WHERE key = %d", dataset, key), ds -> ds.delete(key)){
      @Override
      public String toString() {
        return "delete(\"" + transaction + "\", \"" + dataset + "\", " + key + ")";
      }
    };
  }

  static class ExternalTransactionOperation extends TransactionOperation {

    private final Consumer<DatasetWriterReader<Integer>> tcAction;

    ExternalTransactionOperation(String txn, String dataset, String sql, Consumer<DatasetWriterReader<Integer>> tcAction) {
      super(txn, dataset, sql);
      this.tcAction = tcAction;
    }

    @Override
    public UnaryOperator<Object> tcTransition() {
      return state -> {
        if (state instanceof TransactionController.ReadWriteTransaction) {
          tcAction.accept(((TransactionController.ReadWriteTransaction) state).writerReader(getDataset()));
          return state;
        } else {
          throw new AssertionError("Unexpected state " + state);
        }
      };
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
            ReadWriteTransaction tcTransaction = (ReadWriteTransaction) tcState.get(txn);
            assertThat("From " + txn + " after " + op + " in " + sequence.toString(), tcState(tcTransaction, tables), is(dbState((Connection) dbTransaction, tables)));
          }
        }
        assertThat("From an outside transaction after " + op + " in " + sequence.toString(), tcState(datasetManager, tables), is(dbState(db, tables)));
        assertThat("From the outside directly after " + op + " in " + sequence.toString(), rawTcState(datasetManager, tables), is(dbState(db, tables)));
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
}
