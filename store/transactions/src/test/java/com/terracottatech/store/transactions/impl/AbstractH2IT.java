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

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.transactions.api.TransactionController;
import com.terracottatech.store.transactions.exception.StoreTransactionTimeoutException;
import org.h2.api.ErrorCode;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.terracottatech.store.Record.keyFunction;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.rangeClosed;
import static java.util.stream.Stream.of;

abstract class AbstractH2IT {

  protected static IntCellDefinition CELL = defineInt("cell");

  static abstract class Operation {

    private final String txn;

    Operation(String txn) {
      this.txn = txn;
    }

    public String getTransaction() {
      return txn;
    }

    abstract boolean legalAfter(List<? extends Operation> prior);

    boolean dbExecute(Map<String, Object> state) {
      try {
        state.compute(getTransaction(), (k, v) -> dbTransition().apply(v));
        return true;
      } catch (AssertionError e) {
        Throwable cause = e.getCause();
        if (cause instanceof SQLException && ((SQLException) cause).getErrorCode() == org.h2.api.ErrorCode.LOCK_TIMEOUT_1) {
          return false;
        } else {
          throw e;
        }
      }
    }

    boolean tcExecute(Map<String,Object> state) {
      try {
        state.compute(getTransaction(), (k, v) -> tcTransition().apply(v));
        return true;
      } catch (StoreTransactionTimeoutException t) {
        return false;
      }
    }

    abstract UnaryOperator<Object> dbTransition();

    abstract UnaryOperator<Object> tcTransition();

    @Override
    public final boolean equals(Object obj) {
      return toString().equals(obj.toString());
    }

    @Override
    public final int hashCode() {
      return toString().hashCode();
    }
  }

  static abstract class TransactionBegin extends Operation {

    TransactionBegin(String txn) {
      super(txn);
    }

    @Override
    public boolean legalAfter(List<? extends Operation> prior) {
      //begin must be the first operation for a transaction
      return prior.stream().noneMatch(op -> getTransaction().equals(op.getTransaction())) &&
      //enforce that transactions begin in order by the local identifier to avoid transaction flipped duplicates
          prior.stream().filter(TransactionBegin.class::isInstance).map(TransactionBegin.class::cast)
              .allMatch(begin -> getTransaction().compareTo(begin.getTransaction()) > 0);
    }

    @Override
    public UnaryOperator<Object> dbTransition() {
      return state -> {
        try {
          Connection connection = DriverManager.getConnection(format("jdbc:h2:mem:%s;IFEXISTS=TRUE", (String) state));
          connection.setAutoCommit(false);
          connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
          return connection;
        } catch (SQLException e) {
          throw new AssertionError(e);
        }
      };
    }

    @Override
    public String toString() {
      return "begin(\"" + getTransaction() + "\")";
    }
  }

  static abstract class TransactionEnd extends Operation {

    TransactionEnd(String txn) {
      super(txn);
    }

    @Override
    public boolean legalAfter(List<? extends Operation> prior) {
      //a transaction can only be ended if it has been started
      return prior.stream().anyMatch(op -> getTransaction().equals(op.getTransaction()) && op instanceof TransactionBegin)
          //a transaction can only be ended once
          && prior.stream().noneMatch(op -> getTransaction().equals(op.getTransaction()) && op instanceof TransactionEnd);
    }
  }

  static abstract class TransactionCommit extends TransactionEnd {

    TransactionCommit(String txn) {
      super(txn);
    }

    @Override
    public UnaryOperator<Object> dbTransition() {
      return state -> {
        try {
          ((Connection) state).commit();
          return null;
        } catch (SQLException e) {
          throw new AssertionError(e);
        }
      };
    }

    @Override
    public String toString() {
      return "commit(\"" + getTransaction() + "\")";
    }
  }

  static abstract class TransactionRollback extends TransactionEnd {

    TransactionRollback(String txn) {
      super(txn);
    }
    @Override
    public UnaryOperator<Object> dbTransition() {
      return state -> {
        try {
          ((Connection) state).rollback();
          return null;
        } catch (SQLException e) {
          throw new AssertionError(e);
        }
      };
    }

    @Override
    public String toString() {
      return "rollback(\"" + getTransaction() + "\")";
    }
  }

  static abstract class TransactionOperation extends Operation {

    private final String dataset;
    private final String sql;

    TransactionOperation(String txn, String dataset, String sql) {
      super(txn);
      this.dataset = dataset;
      this.sql = sql;
    }

    public String getDataset() {
      return dataset;
    }

    @Override
    public boolean legalAfter(List<? extends Operation> prior) {
      //transaction operations happen after the transaction begin
      return prior.stream().anyMatch(op -> getTransaction().equals(op.getTransaction()) && op instanceof TransactionBegin)
          //transaction operations happen before the transaction end
          && prior.stream().noneMatch(op -> getTransaction().equals(op.getTransaction()) && op instanceof TransactionEnd)
          //the 'first' dataset must be touched first which prevents dataset flipped duplicates
          && prior.stream().filter(TransactionOperation.class::isInstance).map(TransactionOperation.class::cast)
          .findFirst().map(f -> getDataset().compareTo(f.getDataset()) >= 0).orElse(true);
    }

    @Override
    public UnaryOperator<Object> dbTransition() {
      return state -> {
        try (Statement statement = ((Connection) state).createStatement()) {
          statement.execute(sql);
        } catch (SQLException e) {
          if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY_1) {
            throw new AssertionError(e);
          }
        }
        return state;
      };
    }
  }

  @SuppressWarnings("unchecked")
  protected static <T> Predicate<List<T>> partialOrdering(T... order) {
    return sequence -> {
      Deque<T> stack = new ArrayDeque<>(asList(order));
      for (T obj : sequence) {
        if (stack.peek().equals(obj)) {
          stack.pop();
          if (stack.isEmpty()) {
            return true;
          }
        }
      }
      return false;
    };
  }

  public static Stream<List<Operation>> legalSequences(Set<Operation> operations) {
    return subsets(operations)
        .filter(allTransactionsOpened(operations))
        .filter(allTransactionsTerminated())
        .filter(allResourcesUsed(operations))
        .flatMap(AbstractH2IT::permute)
        .filter(sequence -> {
          for (int i = 0; i < sequence.size(); i++) {
            if (!sequence.get(i).legalAfter(sequence.subList(0, i))) {
              return false;
            }
          }
          return true;
        });
  }

  private static Predicate<Set<Operation>> allTransactionsTerminated() {
    return set -> set.stream().filter(TransactionBegin.class::isInstance)
        .allMatch(begin -> set.stream().filter(TransactionEnd.class::isInstance).map(Operation::getTransaction)
            .filter(begin.getTransaction()::equals).count() == 1);
  }

  private static Predicate<Set<Operation>> allTransactionsOpened(Set<Operation> operations) {
    long total = operations.stream().filter(TransactionBegin.class::isInstance).count();
    return set -> set.stream().filter(TransactionBegin.class::isInstance).count() == total;
  }

  private static Predicate<Set<Operation>> allResourcesUsed(Set<Operation> operations) {
    long total = operations.stream().filter(TransactionOperation.class::isInstance)
        .map(TransactionOperation.class::cast).map(TransactionOperation::getDataset).distinct().count();
    return set -> set.stream().filter(TransactionOperation.class::isInstance).map(TransactionOperation.class::cast)
        .map(TransactionOperation::getDataset).distinct().count() == total;
  }

  private static <T> Stream<Set<T>> subsets(Set<T> objects) {
    List<T> ordered = new ArrayList<>(objects);
    return LongStream.range(0, 1L << ordered.size()).mapToObj(l -> BitSet.valueOf(new long[]{l})).map(bs -> bs.stream().mapToObj(i -> ordered.get(i)).collect(toSet()));
  }

  private static <T> Stream<List<T>> permute(Collection<T> objects) {
    return objects.stream().<UnaryOperator<Stream<List<T>>>>map(
        o ->
            s -> s.flatMap(
                (l -> rangeClosed(0, l.size()).boxed().map(i -> {
                  List<T> r = new ArrayList<>(l);
                  r.add(i, o);
                  return r;
                }))
            )
    ).reduce(identity(), (UnaryOperator<Stream<List<T>>> a, UnaryOperator<Stream<List<T>>> b) -> s -> a.apply(b.apply(s))).apply(of(emptyList()));
  }

  protected static Closeable initializeDatabase(String database, Collection<String> tables) throws SQLException {
    Connection connection = DriverManager.getConnection(format("jdbc:h2:mem:%s", database));
    try (Statement initialize = connection.createStatement()) {
      for (String table : tables) {
        initialize.addBatch(format("CREATE TABLE %s (key INTEGER PRIMARY KEY, cell INTEGER)", table));
      }
      initialize.executeBatch();
    }
    return () -> {
      try {
        try {
          try (Statement destroy = connection.createStatement()) {
            for (String table : tables) {
              destroy.addBatch(format("DROP TABLE %s", table));
            }
            destroy.executeBatch();
          }
        } finally {
          connection.close();
        }
      } catch (SQLException e) {
        throw new AssertionError(e);
      }
    };
  }

  protected static Map<String,Object> initializeDatabaseState(String db, Collection<String> transactions) {
    Map<String, Object> state = new HashMap<>();
    for (String txn : transactions) {
      state.put(txn, db);
    }
    return state;
  }

  protected static Map<String, Map<Integer, Optional<Integer>>> dbState(String database, Collection<String> tables) throws SQLException {
    try (Connection connection = DriverManager.getConnection(format("jdbc:h2:mem:%s;IFEXISTS=TRUE", database))) {
      return dbState(connection, tables);
    }
  }

  protected static Map<String, Map<Integer, Optional<Integer>>> dbState(Connection connection, Collection<String> tables) throws SQLException {
    Map<String, Map<Integer, Optional<Integer>>> dbState = new HashMap<>();
    for (String table : tables) {
      HashMap<Integer, Optional<Integer>> tableState = new HashMap<>();
      dbState.put(table, tableState);
      try (ResultSet rs = connection.createStatement().executeQuery(format("SELECT key, cell FROM %s", table))) {
        while (rs.next()) {
          tableState.put(rs.getInt(1), Optional.ofNullable((Integer) rs.getObject(2)));
        }
      }
    }
    return dbState;
  }


  protected static DatasetManager initializeTcStore(Collection<String> tables) throws StoreException {
    DatasetManager datasetManager = DatasetManager.embedded().offheap("offheap", 16, MemoryUnit.MB).build();
    for (String table : tables) {
      datasetManager.newDataset(table, Type.INT, datasetManager.datasetConfiguration().offheap("offheap"));
    }
    return datasetManager;
  }

  protected static Map<String, Map<Integer, Optional<Integer>>> rawTcState(DatasetManager datasetManager, Collection<String> tables) throws SQLException, StoreException {
    return tables.stream().collect(toMap(t -> t, t -> {
      try (Dataset<Integer> dataset = datasetManager.getDataset(t, Type.INT)) {
        return dataset.reader().records().filter(CELL.exists()).collect(toMap(keyFunction(), CELL.value()));
      } catch (StoreException e) {
        throw new AssertionError(e);
      }
    }));
  }

  @SuppressWarnings("unchecked")
  protected static Map<String, Map<Integer, Optional<Integer>>> tcState(DatasetManager datasetManager, Collection<String> tables) throws Exception {
    TransactionController.ReadOnlyExecutionBuilder builder = TransactionController.createTransactionController(datasetManager, datasetManager.datasetConfiguration().offheap("offheap")).transact();
    for (String dataset : tables) {
      builder = builder.using(dataset, datasetManager.getDataset(dataset, Type.INT).reader());
    }
    return builder.execute(resources ->
        resources.entrySet().stream().collect(toMap(
            entry -> entry.getKey(),
            entry -> ((DatasetReader<Integer>) entry.getValue()).records().collect(toMap(keyFunction(), CELL.value()))))
    );
  }

  @SuppressWarnings("unchecked")
  protected static Map<String, Map<Integer, Optional<Integer>>> tcState(TransactionController.ReadWriteTransaction txn, Collection<String> tables) throws SQLException, StoreException {
    return tables.stream().collect(toMap(identity(), table -> ((DatasetReader<Integer>) txn.writerReader(table)).records().collect(toMap(keyFunction(), CELL.value()))));
  }

  protected static Set<String> transactionsIn(List<Operation> sequence) {
    return sequence.stream().map(Operation::getTransaction).collect(toSet());
  }

  protected static Set<String> tablesIn(List<Operation> sequence) {
    return sequence.stream().filter(TransactionOperation.class::isInstance).map(TransactionOperation.class::cast)
        .map(TransactionOperation::getDataset).collect(toSet());
  }

  protected static <T> Consumer<T> parallelism(int i, Consumer<T> execute) {
    Semaphore semaphore = new Semaphore(i);
    return t -> {
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
      try {
        execute.accept(t);
      } finally {
        semaphore.release();
      }
    };
  }

}
