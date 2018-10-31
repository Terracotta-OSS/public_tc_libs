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

import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.internal.function.Functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static java.util.stream.StreamSupport.stream;

/**
 * A transform that updates a record.
 *
 * @param <K> consumed record key type
 */
@FunctionalInterface
public interface UpdateOperation<K extends Comparable<K>> {

  /**
   * Applies this transform to the given record.
   *
   * @param t the incoming record
   * @return the resultant cells
   */
  Iterable<Cell<?>> apply(Record<K> t);

  /**
   * Creates a transform that outputs the specified set of cells.
   * When used to update a record, this will result in any other cells being deleted from the record.
   *
   * @param <K> consumed record key type
   * @param cells cells to install
   * @return a cell installing transform
   */
  static <K extends Comparable<K>> UpdateOperation<K> install(Cell<?>... cells) {
    return install(asList(cells));
  }

  /**
   * Creates a transform that outputs the specified set of cells.
   * When used to update a record, this will result in any other cells being deleted from the record.
   *
   * @param <K> consumed record key type
   * @param cells a non-{@code null} {@code Iterable} supplying cells to install
   * @return a cell installing transform
   *
   * @throws NullPointerException if {@code cells} is {@code null}
   */
  static <K extends Comparable<K>> UpdateOperation<K> install(Iterable<Cell<?>> cells) {
    requireNonNull(cells);
    return Functions.installUpdateOperation(cells);
  }

  /**
   * Creates a custom transform that applies the given user supplied function.
   *
   * @param <K> consumed record key type
   * @param custom user transform to execute
   * @return a custom transform
   */
  static <K extends Comparable<K>> UpdateOperation<K> custom(Function<Record<K>, Iterable<Cell<?>>> custom) {
    return custom::apply;
  }

  /**
   * Creates a compound transform composed of the given individual cell mutations.
   *
   * @param <K> consumed record key type
   * @param transforms list of transforms to perform
   * @return a compounded transform
   */
  @SuppressWarnings("unchecked")
  static <K extends Comparable<K>> UpdateOperation<K> allOf(CellUpdateOperation<?, ?>... transforms) {
    return Functions.allOfUpdateOperation(Arrays.asList(transforms));
  }

  /**
   * Creates a compound transform composed of the given individual cell mutations.
   *
   * @param <K> consumed record key type
   * @param transforms list of transforms to perform
   * @return a compounded transform
   */
  @SuppressWarnings("unchecked")
  static <K extends Comparable<K>> UpdateOperation<K> allOf(List<? extends CellUpdateOperation<?, ?>> transforms) {
    return Functions.allOfUpdateOperation(new ArrayList<>(transforms));
  }

  /**
   * A single cell transform.
   *
   * @param <K> consumed record key type
   * @param <T> cell type
   */
  interface CellUpdateOperation<K extends Comparable<K>, T> extends UpdateOperation<K> {

    /**
     * Returns the cell definition modified by this transform.
     *
     * @return the modified definition
     */
    CellDefinition<T> definition();

    /**
     * Returns the cell to be written or an empty optional to remove.
     *
     * @return the cell to be written
     */
    Function<Record<?>, Optional<Cell<T>>> cell();

    @Override
    default Iterable<Cell<?>> apply(Record<K> t) {
      return concat(
              stream(t.spliterator(), false).filter(c -> !c.definition().equals(definition())),
              cell().apply(t).map(Stream::of).orElse(empty())
      ).collect(toList());
    }
  }

  /**
   * A builder for a transform that writes a single cell.
   * @param <T> the type of the cell
   */
  interface WriteOperationBuilder<T> {
    /**
     * Creates a transform that writes a cell with the supplied definition and value.
     * @param value cell value
     * @param <K> the type of the key
     * @return a single cell writing transform
     */
    <K extends Comparable<K>> CellUpdateOperation<K, T> value(T value);

    /**
     * Creates a transform that writes a cell with the supplied definition and value derived by applying the function
     * to the existing record.
     * @param function cell value function
     * @param <K> the type of the key
     * @return a single cell writing transform
     */
    <K extends Comparable<K>> CellUpdateOperation<K, T> resultOf(Function<Record<?>, T> function);
  }

  /**
   * A builder for a transform that writes a single boolean cell.
   */
  interface BoolWriteOperationBuilder extends WriteOperationBuilder<Boolean> {
    /**
     * Creates a transform that writes a cell with the supplied definition and value derived by applying the predicate
     * to the existing record.
     * @param predicate cell value predicate
     * @param <K> the type of the key
     * @return a single cell writing transform
     */
    <K extends Comparable<K>> CellUpdateOperation<K, Boolean> boolResultOf(Predicate<Record<?>> predicate);
  }

  /**
   * A builder for a transform that writes a single int cell.
   */
  interface IntWriteOperationBuilder extends WriteOperationBuilder<Integer> {
    /**
     * Creates a transform that writes a cell with the supplied definition and value derived by applying the function
     * to the existing record.
     * @param toIntFunction cell value function
     * @param <K> the type of the key
     * @return a single cell writing transform
     */
    <K extends Comparable<K>> CellUpdateOperation<K, Integer> intResultOf(ToIntFunction<Record<?>> toIntFunction);
  }

  /**
   * A builder for a transform that writes a single long cell.
   */
  interface LongWriteOperationBuilder extends WriteOperationBuilder<Long> {
    /**
     * Creates a transform that writes a cell with the supplied definition and value derived by applying the function
     * to the existing record.
     * @param toLongFunction cell value function
     * @param <K> the type of the key
     * @return a single cell writing transform
     */
    <K extends Comparable<K>> CellUpdateOperation<K, Long> longResultOf(ToLongFunction<Record<?>> toLongFunction);
  }

  /**
   * A builder for a transform that writes a single double cell.
   */
  interface DoubleWriteOperationBuilder extends WriteOperationBuilder<Double> {
    /**
     * Creates a transform that writes a cell with the supplied definition and value derived by applying the function
     * to the existing record.
     * @param toDoubleFunction cell value function
     * @param <K> the type of the key
     * @return a single cell writing transform
     */
    <K extends Comparable<K>> CellUpdateOperation<K, Double> doubleResultOf(ToDoubleFunction<Record<?>> toDoubleFunction);
  }

  static <T> WriteOperationBuilder<T> write(CellDefinition<T> definition) {
    return new WriteOperationBuilder<T>() {
      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, T> value(T value) {
        return UpdateOperation.write(definition.newCell(value));
      }

      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, T> resultOf(Function<Record<?>, T> function) {
        return Functions.writeUpdateOperation(definition, function);
      }
    };
  }

  static BoolWriteOperationBuilder write(BoolCellDefinition definition) {
    return new BoolWriteOperationBuilder() {
      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Boolean> value(Boolean value) {
        return UpdateOperation.write(definition.newCell(value));
      }

      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Boolean> resultOf(Function<Record<?>, Boolean> function) {
        return Functions.writeUpdateOperation(definition, function);
      }

      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Boolean> boolResultOf(Predicate<Record<?>> predicate) {
        return Functions.writeUpdateOperation(definition, predicate);
      }
    };
  }

  static IntWriteOperationBuilder write(IntCellDefinition definition) {
    return new IntWriteOperationBuilder() {
      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Integer> value(Integer value) {
        return UpdateOperation.write(definition.newCell(value));
      }

      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Integer> resultOf(Function<Record<?>, Integer> function) {
        return Functions.writeUpdateOperation(definition, function);
      }

      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Integer> intResultOf(ToIntFunction<Record<?>> toIntFunction) {
        return Functions.writeUpdateOperation(definition, toIntFunction);
      }
    };
  }

  static LongWriteOperationBuilder write(LongCellDefinition definition) {
    return new LongWriteOperationBuilder() {
      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Long> value(Long value) {
        return UpdateOperation.write(definition.newCell(value));
      }

      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Long> resultOf(Function<Record<?>, Long> function) {
        return Functions.writeUpdateOperation(definition, function);
      }

      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Long> longResultOf(ToLongFunction<Record<?>> toLongFunction) {
        return Functions.writeUpdateOperation(definition, toLongFunction);
      }
    };
  }

  static DoubleWriteOperationBuilder write(DoubleCellDefinition definition) {
    return new DoubleWriteOperationBuilder() {
      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Double> value(Double value) {
        return UpdateOperation.write(definition.newCell(value));
      }

      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Double> resultOf(Function<Record<?>, Double> function) {
        return Functions.writeUpdateOperation(definition, function);
      }

      @Override
      public <K extends Comparable<K>> CellUpdateOperation<K, Double> doubleResultOf(ToDoubleFunction<Record<?>> toDoubleFunction) {
        return Functions.writeUpdateOperation(definition, toDoubleFunction);
      }
    };
  }

  /**
   * Creates a transform that writes the given cell.
   *
   * @param <K> consumed record key type
   * @param <T> cell type
   * @param cell the cell to write
   * @return a single cell writing transform
   */
  static <K extends Comparable<K>, T> CellUpdateOperation<K, T> write(Cell<T> cell) {
    return Functions.writeUpdateOperation(cell);
  }

  /**
   * Creates a transform that writes a cell of the given name.
   *
   * @param <K> consumed record key type
   * @param <T> cell type
   * @param cellName cell name
   * @param value cell value
   * @return a single cell writing transform
   */
  static <K extends Comparable<K>, T> CellUpdateOperation<K, T> write(String cellName, T value) {
    return write(Cell.cell(cellName, value));
  }

  /**
   * Create a transform that removes a cell.
   *
   * @param <K> consumed record key type
   * @param <T> cell type
   * @param definition cell definition to remove
   * @return a single cell removing transform
   */
  static <K extends Comparable<K>, T> CellUpdateOperation<K, T> remove(CellDefinition<T> definition) {
    return Functions.removeUpdateOperation(definition);
  }

  /**
   * Creates a bi-function that returns the result of the first argument applied to the given function.
   *
   * @param <T> consumed input type
   * @param <U> unconsumed input type
   * @param <R> result type
   * @param function mapping function
   * @return an input mapping bi-function
   */
  static <T, U, R> BiFunction<T, U, R> input(Function<T, R> function) {
    return Functions.inputMapper(function);
  }

  /**
   * Creates a bi-function that returns the result of the second argument applied to the given function.
   *
   * @param <T> unconsumed input type
   * @param <U> consumed input type
   * @param <R> result type
   * @param function mapping function
   * @return an output mapping bi-function
   */
  static <T, U, R> BiFunction<T, U, R> output(Function<U, R> function) {
    return Functions.outputMapper(function);
  }
}
