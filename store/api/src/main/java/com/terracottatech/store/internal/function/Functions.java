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
package com.terracottatech.store.internal.function;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.ComparableCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.function.BuildableComparableFunction;
import com.terracottatech.store.function.BuildableComparableOptionalFunction;
import com.terracottatech.store.function.BuildableFunction;
import com.terracottatech.store.function.BuildableOptionalFunction;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.function.BuildableStringFunction;
import com.terracottatech.store.function.BuildableStringOptionalFunction;
import com.terracottatech.store.function.BuildableToDoubleFunction;
import com.terracottatech.store.function.BuildableToIntFunction;
import com.terracottatech.store.function.BuildableToLongFunction;

import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;

/**
 * This class consists of static methods for creating functions.
 */
public class Functions {

  private static final FunctionProvider FUNCTION_PROVIDER;

  static {
    FUNCTION_PROVIDER = FunctionProvider.getFunctionProvider();
  }

  /**
   * Create {@link BuildablePredicate} that tests if a {@link Record} contains a cell whose definition matches the passed one.
   * @param cellDefinition the cell definition.
   * @return a {@link BuildablePredicate}.
   */
  public static BuildablePredicate<Record<?>> cellDefinitionExists(CellDefinition<?> cellDefinition) {
    return FUNCTION_PROVIDER.cellDefinitionExists(cellDefinition);
  }

  /**
   * Create an {@link UpdateOperation} that installs {@link Cell}s.
   * @param cells The cells to install.
   * @param <K> the {@link Comparable} type of the {@link Record}.
   * @return a {@link UpdateOperation}.
   */
  public static <K extends Comparable<K>> UpdateOperation<K> installUpdateOperation(Iterable<Cell<?>> cells) {
    return FUNCTION_PROVIDER.installUpdateOperation(cells);
  }

  /**
   * Create an {@link UpdateOperation} that writes a {@link Cell}.
   * @param cell the the cell to write.
   * @param <K> the {@link Comparable} type of the {@link Record}.
   * @param <T> the type of the cell to write.
   * @return a {@link UpdateOperation.CellUpdateOperation}.
   */
  public static <K extends Comparable<K>, T> UpdateOperation.CellUpdateOperation<K, T> writeUpdateOperation(Cell<T> cell) {
    return FUNCTION_PROVIDER.writeUpdateOperation(cell);
  }

  /**
   * Create an {@link UpdateOperation} that writes a value computed by a {@link Function} taking the record as its parameter.
   * @param definition the {@link CellDefinition} of the cell to write.
   * @param value the function that is responsible for generating the value to write.
   * @param <K> the {@link Comparable} type of the {@link Record}.
   * @param <T> the type of the cell to write.
   * @return a {@link UpdateOperation.CellUpdateOperation}.
   */
  public static <K extends Comparable<K>, T> UpdateOperation.CellUpdateOperation<K, T> writeUpdateOperation(CellDefinition<T> definition, Function<Record<?>, T> value) {
    return FUNCTION_PROVIDER.writeUpdateOperation(definition, value);
  }

  /**
   * Create an {@link UpdateOperation} that writes a value computed by a {@link ToIntFunction} taking the record as its parameter.
   * @param definition the {@link CellDefinition} of the cell to write.
   * @param value the function that is responsible for generating the value to write.
   * @param <K> the {@link Comparable} type of the {@link Record}.
   * @return a {@link UpdateOperation.CellUpdateOperation}.
   */
  public static <K extends Comparable<K>> UpdateOperation.CellUpdateOperation<K, Integer> writeUpdateOperation(CellDefinition<Integer> definition, ToIntFunction<Record<?>> value) {
    return FUNCTION_PROVIDER.writeUpdateOperation(definition, value);
  }

  /**
   * Create an {@link UpdateOperation} that writes a value computed by a {@link ToLongFunction} taking the record as its parameter.
   * @param definition the {@link CellDefinition} of the cell to write.
   * @param value the function that is responsible for generating the value to write.
   * @param <K> the {@link Comparable} type of the {@link Record}.
   * @return a {@link UpdateOperation.CellUpdateOperation}.
   */
  public static <K extends Comparable<K>> UpdateOperation.CellUpdateOperation<K, Long> writeUpdateOperation(CellDefinition<Long> definition, ToLongFunction<Record<?>> value) {
    return FUNCTION_PROVIDER.writeUpdateOperation(definition, value);
  }

  /**
   * Create an {@link UpdateOperation} that writes a value computed by a {@link ToDoubleFunction} taking the record as its parameter.
   * @param definition the {@link CellDefinition} of the cell to write.
   * @param value the function that is responsible for generating the value to write.
   * @param <K> the {@link Comparable} type of the {@link Record}.
   * @return a {@link UpdateOperation.CellUpdateOperation}.
   */
  public static <K extends Comparable<K>> UpdateOperation.CellUpdateOperation<K, Double> writeUpdateOperation(CellDefinition<Double> definition, ToDoubleFunction<Record<?>> value) {
    return FUNCTION_PROVIDER.writeUpdateOperation(definition, value);
  }

  public static <K extends Comparable<K>> UpdateOperation.CellUpdateOperation<K, Boolean> writeUpdateOperation(CellDefinition<Boolean> definition, Predicate<Record<?>> value) {
    return FUNCTION_PROVIDER.writeUpdateOperation(definition, value);
  }

  /**
   * Create an {@link UpdateOperation} that removes a {@link Cell}.
   * @param definition the {@link CellDefinition} of the cell to remove.
   * @param <K> the {@link Comparable} type of the {@link Record}.
   * @param <T> the type of the cell to remove.
   * @return a {@link UpdateOperation.CellUpdateOperation}.
   */
  public static <K extends Comparable<K>, T> UpdateOperation.CellUpdateOperation<K, T> removeUpdateOperation(CellDefinition<T> definition) {
    return FUNCTION_PROVIDER.removeUpdateOperation(definition);
  }

  /**
   * Create an {@link UpdateOperation} that performs all specified {@link UpdateOperation.CellUpdateOperation}s.
   * @param transforms the transformations to apply
   * @param <K> the {@link Comparable} type of the {@link Record}.
   * @return an {@link UpdateOperation}.
   */
  public static <K extends Comparable<K>> UpdateOperation<K> allOfUpdateOperation(List<UpdateOperation.CellUpdateOperation<?, ?>> transforms) {
    return FUNCTION_PROVIDER.allOfUpdateOperation(transforms);
  }

  /**
   * Return a {@link BuildableFunction} that extracts the value of a cell from a record or
   * uses the given default if the cell is absent.
   * @param definition the {@link CellDefinition} of the cell to extract the value of.
   * @param otherwise default value for absent cells.
   * @return a cell extracting {@link BuildableFunction}.
   */
  public static <T> BuildableFunction<Record<?>, T> valueOr(CellDefinition<T> definition, T otherwise) {
    return FUNCTION_PROVIDER.valueOr(definition, otherwise);
  }

  /**
   * Return a {@link BuildableFunction} that extracts the value of a cell from a record or
   * throws a {@link NoSuchElementException} if the cell is absent.
   * @param definition the {@link CellDefinition} of the cell to extract the value of.
   * @return a cell extracting {@link BuildableFunction}.
   */
  public static <T> BuildableFunction<Record<?>, T> valueOrFail(CellDefinition<T> definition) {
    return FUNCTION_PROVIDER.valueOr(definition, null);
  }

  /**
   * Return a {@link BuildableComparableFunction} that extracts the value of a cell from a record or
   * uses the given default if the cell is absent.
   * @param definition the {@link ComparableCellDefinition} of the cell to extract the value of.
   * @param otherwise default value for absent cells.
   * @return a cell extracting {@link BuildableComparableFunction}.
   */
  public static <T extends Comparable<T>> BuildableComparableFunction<Record<?>, T> comparableValueOr(ComparableCellDefinition<T> definition, T otherwise) {
    return FUNCTION_PROVIDER.comparableValueOr(definition, otherwise);
  }

  /**
   * Return a {@link BuildableComparableFunction} that extracts the value of a cell from a record or
   * throws a {@link NoSuchElementException} if the cell is absent.
   * @param definition the {@link ComparableCellDefinition} of the cell to extract the value of.
   * @return a cell extracting {@link BuildableComparableFunction}.
   */
  public static <T extends Comparable<T>> BuildableComparableFunction<Record<?>, T> comparableValueOrFail(ComparableCellDefinition<T> definition) {
    return FUNCTION_PROVIDER.comparableValueOr(definition, null);
  }

  /**
   * Return a {@link BuildableToIntFunction} that extracts the value of a cell from a record or
   * uses the given default if the cell is absent.
   * @param definition the {@link IntCellDefinition} of the cell to extract the value of.
   * @param otherwise default value for absent cells.
   * @return a cell extracting {@link BuildableToIntFunction}.
   */
  public static BuildableToIntFunction<Record<?>> intValueOr(IntCellDefinition definition, int otherwise) {
    return FUNCTION_PROVIDER.intValueOr(definition, otherwise);
  }

  /**
   * Return a {@link BuildableToIntFunction} that extracts the value of a cell from a record or
   * throws a {@link NoSuchElementException} if the cell is absent.
   * @param definition the {@link IntCellDefinition} of the cell to extract the value of.
   * @return a cell extracting {@link BuildableToIntFunction}.
   */
  public static BuildableToIntFunction<Record<?>> intValueOrFail(IntCellDefinition definition) {
    return FUNCTION_PROVIDER.intValueOr(definition, null);
  }

  /**
   * Return a {@link BuildableToLongFunction} that extracts the value of a cell from a record or
   * uses the given default if the cell is absent.
   * @param definition the {@link LongCellDefinition} of the cell to extract the value of.
   * @param otherwise default value for absent cells.
   * @return a cell extracting {@link BuildableToLongFunction}.
   */
  public static BuildableToLongFunction<Record<?>> longValueOr(LongCellDefinition definition, long otherwise) {
    return FUNCTION_PROVIDER.longValueOr(definition, otherwise);
  }

  /**
   * Return a {@link BuildableToLongFunction} that extracts the value of a cell from a record or
   * throws a {@link NoSuchElementException} if the cell is absent.
   * @param definition the {@link LongCellDefinition} of the cell to extract the value of.
   * @return a cell extracting {@link BuildableToLongFunction}.
   */
  public static BuildableToLongFunction<Record<?>> longValueOrFail(LongCellDefinition definition) {
    return FUNCTION_PROVIDER.longValueOr(definition, null);
  }

  /**
   * Return a {@link BuildableToDoubleFunction} that extracts the value of a cell from a record or
   * uses the given default if the cell is absent.
   * @param definition the {@link DoubleCellDefinition} of the cell to extract the value of.
   * @param otherwise default value for absent cells.
   * @return a cell extracting {@link BuildableToDoubleFunction}.
   */
  public static BuildableToDoubleFunction<Record<?>> doubleValueOr(DoubleCellDefinition definition, double otherwise) {
    return FUNCTION_PROVIDER.doubleValueOr(definition, otherwise);
  }

  /**
   * Return a {@link BuildableToDoubleFunction} that extracts the value of a cell from a record or
   * throws a {@link NoSuchElementException} if the cell is absent.
   * @param definition the {@link DoubleCellDefinition} of the cell to extract the value of.
   * @return a cell extracting {@link BuildableToDoubleFunction}.
   */
  public static BuildableToDoubleFunction<Record<?>> doubleValueOrFail(DoubleCellDefinition definition) {
    return FUNCTION_PROVIDER.doubleValueOr(definition, null);
  }

  /**
   * Return a {@link BuildableStringFunction} that extracts the value of a cell from a record or
   * uses the given default if the cell is absent.
   * @param definition the {@link StringCellDefinition} of the cell to extract the value of.
   * @param otherwise default value for absent cells.
   * @return a cell extracting {@link BuildableStringFunction}.
   */
  public static BuildableStringFunction<Record<?>> stringValueOr(StringCellDefinition definition, String otherwise) {
    return FUNCTION_PROVIDER.stringValueOr(definition, otherwise);
  }

  /**
   * Return a {@link BuildableStringFunction} that extracts the value of a cell from a record or
   * throws a {@link NoSuchElementException} if the cell is absent.
   * @param definition the {@link StringCellDefinition} of the cell to extract the value of.
   * @return a cell extracting {@link BuildableStringFunction}.
   */
  public static BuildableStringFunction<Record<?>> stringValueOrFail(StringCellDefinition definition) {
    return FUNCTION_PROVIDER.stringValueOr(definition, null);
  }

  /**
   * Create a {@link BuildableOptionalFunction} that extracts the value of the supplied cell from a record.
   *
   * @param definition cell to extract
   * @param <T> type of the extracted cell
   * @return a cell extraction function
   */
  public static <T> BuildableOptionalFunction<Record<?>, T> extractPojo(CellDefinition<T> definition) {
    return FUNCTION_PROVIDER.extractPojo(definition);
  }

  /**
   * Create a {@link BuildableComparableOptionalFunction} that extracts the value of the supplied cell from a record.
   *
   * @param definition cell to extract
   * @param <T> type of the extracted cell
   * @return a cell extraction function
   */
  public static <T extends Comparable<T>> BuildableComparableOptionalFunction<Record<?>, T> extractComparable(CellDefinition<T> definition) {
    return FUNCTION_PROVIDER.extractComparable(definition);
  }

  /**
   * Create a {@link BuildableStringOptionalFunction} that extracts the value of the supplied cell from a record.
   *
   * @param definition cell to extract
   * @return a string cell extraction function
   */
  public static BuildableStringOptionalFunction<Record<?>> extractString(CellDefinition<String> definition) {
    return FUNCTION_PROVIDER.extractString(definition);
  }

  /**
   * Create a {@link Collector} that counts the elements in the stream.
   * @param <T> the element type
   * @return a counting {@code Collector}
   */
  public static <T> Collector<T, ?, Long> countingCollector() {
    return FUNCTION_PROVIDER.countingCollector();
  }

  /**
   * Create a {@link Collector} grouping elements according to a classification function, and
   * then reducing using the specified downstream Collector.
   *
   * @param classifier a classifier function mapping input elements to keys
   * @param downstream a Collector implementing the downstream reduction
   * @param <T> the type of the input elements
   * @param <K> the type of the keys
   * @param <A> the intermediate accumulation type of the downstream collector
   * @param <D> the result type of the downstream reduction
   * @return a Collector implementing the cascaded group-by operation.
   */
  public static <T, K, A, D> Collector<T, ?, Map<K, D>> groupingByCollector(
          Function<? super T, ? extends K> classifier, Collector<? super T, A, D> downstream) {
    return FUNCTION_PROVIDER.groupingByCollector(classifier, downstream);
  }

  /**
   * Create a concurrent {@link Collector} grouping elements according to a classification function, and
   * then reducing using the specified downstream Collector.
   *
   * @param classifier a classifier function mapping input elements to keys
   * @param downstream a Collector implementing the downstream reduction
   * @param <T> the type of the input elements
   * @param <K> the type of the keys
   * @param <A> the intermediate accumulation type of the downstream collector
   * @param <D> the result type of the downstream reduction
   * @return a concurrent, unordered Collector implementing the cascaded group-by operation.
   */
  public static <T, K, A, D> Collector<T, ?, ConcurrentMap<K, D>> groupingByConcurrentCollector(
          Function<? super T, ? extends K> classifier, Collector<? super T, A, D> downstream) {
    return FUNCTION_PROVIDER.groupingByConcurrentCollector(classifier, downstream);
  }

  /**
   * Returns a {@code Collector} which partitions the input elements according
   * to a {@code Predicate}, reduces the values in each partition according to
   * another {@code Collector}, and organizes them into a
   * {@code Map<Boolean, D>} whose values are the result of the downstream
   * reduction.
   *
   * @param <T> the type of the input elements
   * @param <A> the intermediate accumulation type of the downstream collector
   * @param <D> the result type of the downstream reduction
   * @param predicate a predicate used for classifying input elements
   * @param downstream a {@code Collector} implementing the downstream
   *                   reduction
   * @return a {@code Collector} implementing the cascaded partitioning
   *         operation
   */
  public static <T, D, A> Collector<T, ?, Map<Boolean, D>> partitioningCollector(
          Predicate<? super T> predicate, Collector<? super T, A, D> downstream) {
    return FUNCTION_PROVIDER.partitioningCollector(predicate, downstream);
  }

  /**
   * Adapts a {@code Collector} accepting elements of type {@code U} to one
   * accepting elements of type {@code T} by applying a mapping function to
   * each input element before accumulation.
   *
   * @param <T> the type of the input elements
   * @param <U> type of elements accepted by downstream collector
   * @param <A> intermediate accumulation type of the downstream collector
   * @param <R> result type of collector
   * @param mapper a function to be applied to the input elements
   * @param downstream a collector which will accept mapped values
   * @return a collector which applies the mapping function to the input
   * elements and provides the mapped results to the downstream collector
   */
  public static <T, U, A, R> Collector<T, ?, R> mappingCollector(Function<? super T, ? extends U> mapper, Collector<? super U, A, R> downstream) {
    return FUNCTION_PROVIDER.mappingCollector(mapper, downstream);
  }

  /**
   * Adapts a Collector to one accepting elements of the same type T by
   * applying the predicate to each input element and only accumulating if
   * the predicate returns true.
   *
   * @param predicate a predicate to be applied to the input elements
   * @param downstream a collector which will accept values that match the predicate
   * @param <T> the type of the input elements
   * @param <A> intermediate accumulation type of the downstream collector
   * @param <R> result type of collector
   * @return a collector which applies the predicate to the input elements and
   * provides matching elements to the downstream collector
   */
  public static <T, A, R> Collector<T, A, R> filteringCollector(Predicate<? super T> predicate, Collector<T, A, R> downstream) {
    return FUNCTION_PROVIDER.filteringCollector(predicate, downstream);
  }

  /**
   * Create a {@link Collector} that computes {@link IntSummaryStatistics} from a {@link ToIntFunction} mapper.
   *
   * @param mapper the mapper
   * @param <T> type of the mapper
   * @return a {@link IntSummaryStatistics} computing collector
   */
  public static <T> Collector<T, ?, IntSummaryStatistics> summarizingIntCollector(ToIntFunction<? super T> mapper) {
    return FUNCTION_PROVIDER.summarizingIntCollector(mapper);
  }

  /**
   * Create a {@link Collector} that computes {@link LongSummaryStatistics} from a {@link ToLongFunction} mapper.
   *
   * @param mapper the mapper
   * @param <T> type of the mapper
   * @return a {@link LongSummaryStatistics} computing collector
   */
  public static <T> Collector<T, ?, LongSummaryStatistics> summarizingLongCollector(ToLongFunction<? super T> mapper) {
    return FUNCTION_PROVIDER.summarizingLongCollector(mapper);
  }

  /**
   * Create a {@link Collector} that computes {@link DoubleSummaryStatistics} from a {@link ToDoubleFunction} mapper.
   *
   * @param mapper the mapper
   * @param <T> type of the mapper
   * @return a {@link DoubleSummaryStatistics} computing collector
   */
  public static <T> Collector<T, ?, DoubleSummaryStatistics> summarizingDoubleCollector(ToDoubleFunction<? super T> mapper) {
    return FUNCTION_PROVIDER.summarizingDoubleCollector(mapper);
  }

  /**
   * Return a {@link BuildableFunction} that extracts the key of a record.
   * @return a key extracting {@link BuildableFunction}.
   */
  public static <K extends Comparable<K>> BuildableComparableFunction<Record<K>, K> recordKeyFunction() {
    return FUNCTION_PROVIDER.recordKeyFunction();
  }

  /**
   * Gets a {@link Function} that returns its input.
   *
   * @param <T> the input and result type
   *
   * @return an <i>identity</i> {@code Function} instance
   */
  public static <T> Function<T, T> identityFunction() {
    return FUNCTION_PROVIDER.identityFunction();
  }

  /**
   * Create a {@link BiFunction} that applies the given function to the first argument to the
   * {@code BiFunction}.
   *
   * @param inputMappingFunction the function to apply to the first {@code BiFunction} argument
   * @param <T> the first {@code BiFunction} argument type
   * @param <U> the second {@code BiFunction} argument type
   * @param <R> the function result type
   *
   * @return a {@code BiFunction} instance to apply the specified function
   */
  public static <T, U, R> BiFunction<T, U, R> inputMapper(Function<T, R> inputMappingFunction) {
    return FUNCTION_PROVIDER.inputMapper(inputMappingFunction);
  }

  /**
   * Create a {@link BiFunction} that applies the given function to the second argument to the
   * {@code BiFunction}.
   *
   * @param outputMappingFunction the function to apply to the second {@code BiFunction} argument
   * @param <T> the first {@code BiFunction} argument type
   * @param <U> the second {@code BiFunction} argument type
   * @param <R> the function result type
   *
   * @return a {@code BiFunction} instance to apply the specified function
   */
  public static <T, U, R> BiFunction<T, U, R> outputMapper(Function<U, R> outputMappingFunction) {
    return FUNCTION_PROVIDER.outputMapper(outputMappingFunction);
  }

  public static <T> BuildableFunction<Tuple<T, ?>, T> tupleFirst() {
    return FUNCTION_PROVIDER.tupleFirst();
  }

  public static <U> BuildableFunction<Tuple<?, U>, U> tupleSecond() {
    return FUNCTION_PROVIDER.tupleSecond();
  }

  public static <T> Consumer<T> log(String message, List<Function<? super T, ?>> mappers) {
    return FUNCTION_PROVIDER.log(message, mappers);
  }
}
