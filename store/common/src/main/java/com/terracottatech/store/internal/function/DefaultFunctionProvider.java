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
import com.terracottatech.store.UpdateOperation.CellUpdateOperation;
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
import com.terracottatech.store.intrinsics.CellDefinitionExists;
import com.terracottatech.store.intrinsics.IdentityFunction;
import com.terracottatech.store.intrinsics.InputMapper;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicCollector;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicToDoubleFunction;
import com.terracottatech.store.intrinsics.IntrinsicToIntFunction;
import com.terracottatech.store.intrinsics.IntrinsicToLongFunction;
import com.terracottatech.store.intrinsics.OutputMapper;
import com.terracottatech.store.intrinsics.impl.AllOfOperation;
import com.terracottatech.store.intrinsics.impl.CellExtractor;
import com.terracottatech.store.intrinsics.impl.CellValue;
import com.terracottatech.store.intrinsics.impl.ConcurrentGroupingCollector;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.intrinsics.impl.CountingCollector;
import com.terracottatech.store.intrinsics.impl.DefaultGroupingCollector;
import com.terracottatech.store.intrinsics.impl.FilteringCollector;
import com.terracottatech.store.intrinsics.impl.InstallOperation;
import com.terracottatech.store.intrinsics.impl.IntrinsicLogger;
import com.terracottatech.store.intrinsics.impl.MappingCollector;
import com.terracottatech.store.intrinsics.impl.PartitioningCollector;
import com.terracottatech.store.intrinsics.impl.PredicateFunctionAdapter;
import com.terracottatech.store.intrinsics.impl.RecordKey;
import com.terracottatech.store.intrinsics.impl.RemoveOperation;
import com.terracottatech.store.intrinsics.impl.SummarizingDoubleCollector;
import com.terracottatech.store.intrinsics.impl.SummarizingIntCollector;
import com.terracottatech.store.intrinsics.impl.SummarizingLongCollector;
import com.terracottatech.store.intrinsics.impl.ToDoubleFunctionAdapter;
import com.terracottatech.store.intrinsics.impl.ToIntFunctionAdapter;
import com.terracottatech.store.intrinsics.impl.ToLongFunctionAdapter;
import com.terracottatech.store.intrinsics.impl.TupleIntrinsic;
import com.terracottatech.store.intrinsics.impl.WriteOperation;

import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;

public class DefaultFunctionProvider implements FunctionProvider {

  @Override
  public BuildablePredicate<Record<?>> cellDefinitionExists(CellDefinition<?> cellDefinition) {
    return new CellDefinitionExists(cellDefinition);
  }

  @Override
  public <K extends Comparable<K>> UpdateOperation<K> installUpdateOperation(Iterable<Cell<?>> cells) {
    return new InstallOperation<>(cells);
  }

  @Override
  public <K extends Comparable<K>, T> CellUpdateOperation<K, T> writeUpdateOperation(Cell<T> cell) {
    return new WriteOperation<>(cell.definition(), new Constant<>(cell.value()));
  }

  @Override
  public <K extends Comparable<K>, T> CellUpdateOperation<K, T> writeUpdateOperation(CellDefinition<T> definition, Function<Record<?>, T> value) {
    if (value instanceof IntrinsicFunction<?, ?>) {
      return new WriteOperation<>(definition, (IntrinsicFunction<Record<?>, T>) value);
    } else {
      return FunctionProvider.super.writeUpdateOperation(definition, value);
    }
  }

  @Override
  public <K extends Comparable<K>> CellUpdateOperation<K, Integer> writeUpdateOperation(CellDefinition<Integer> definition, ToIntFunction<Record<?>> value) {
    if (value instanceof IntrinsicToIntFunction<?>) {
      return new WriteOperation<>(definition, new ToIntFunctionAdapter<>((IntrinsicToIntFunction<Record<?>>) value));
    } else {
      return FunctionProvider.super.writeUpdateOperation(definition, value);
    }
  }

  @Override
  public <K extends Comparable<K>> CellUpdateOperation<K, Long> writeUpdateOperation(CellDefinition<Long> definition, ToLongFunction<Record<?>> value) {
    if (value instanceof IntrinsicToLongFunction<?>) {
      return new WriteOperation<>(definition, new ToLongFunctionAdapter<>((IntrinsicToLongFunction<Record<?>>) value));
    } else {
      return FunctionProvider.super.writeUpdateOperation(definition, value);
    }
  }

  @Override
  public <K extends Comparable<K>> CellUpdateOperation<K, Double> writeUpdateOperation(CellDefinition<Double> definition, ToDoubleFunction<Record<?>> value) {
    if (value instanceof IntrinsicToDoubleFunction<?>) {
      return new WriteOperation<>(definition, new ToDoubleFunctionAdapter<>((IntrinsicToDoubleFunction<Record<?>>) value));
    } else {
      return FunctionProvider.super.writeUpdateOperation(definition, value);
    }
  }

  @Override
  public <K extends Comparable<K>> CellUpdateOperation<K, Boolean> writeUpdateOperation(CellDefinition<Boolean> definition, Predicate<Record<?>> value) {
    if (value instanceof IntrinsicPredicate<?>) {
      return new WriteOperation<>(definition, new PredicateFunctionAdapter<>((IntrinsicPredicate<Record<?>>) value));
    } else {
      return FunctionProvider.super.writeUpdateOperation(definition, value);
    }
  }

  @Override
  public <K extends Comparable<K>, T> CellUpdateOperation<K, T> removeUpdateOperation(CellDefinition<T> definition) {
    return new RemoveOperation<>(definition);
  }

  @Override
  public <K extends Comparable<K>> UpdateOperation<K> allOfUpdateOperation(List<CellUpdateOperation<?, ?>> transforms) {
    boolean fullyIntrinsic = true;
    for (CellUpdateOperation<?, ?> transform : transforms) {
      fullyIntrinsic &= transform instanceof Intrinsic;
    }

    if (fullyIntrinsic) {
      return new AllOfOperation<>(transforms);
    } else {
      return FunctionProvider.super.allOfUpdateOperation(transforms);
    }
  }

  @Override
  public <T> BuildableFunction<Record<?>, T> valueOr(CellDefinition<T> definition, T otherwise) {
    return new CellValue<>(definition, otherwise);
  }

  @Override
  public <T extends Comparable<T>> BuildableComparableFunction<Record<?>, T> comparableValueOr(ComparableCellDefinition<T> definition, T otherwise) {
    return new CellValue.ComparableCellValue<>(definition, otherwise);
  }

  @Override
  public BuildableToIntFunction<Record<?>> intValueOr(IntCellDefinition definition, Integer otherwise) {
    return new CellValue.IntCellValue(definition, otherwise);
  }

  @Override
  public BuildableToLongFunction<Record<?>> longValueOr(LongCellDefinition definition, Long otherwise) {
    return new CellValue.LongCellValue(definition, otherwise);
  }

  @Override
  public BuildableToDoubleFunction<Record<?>> doubleValueOr(DoubleCellDefinition definition, Double otherwise) {
    return new CellValue.DoubleCellValue(definition, otherwise);
  }

  @Override
  public BuildableStringFunction<Record<?>> stringValueOr(StringCellDefinition definition, String otherwise) {
    return new CellValue.StringCellValue(definition, otherwise);
  }

  @Override
  public <T> BuildableOptionalFunction<Record<?>, T> extractPojo(CellDefinition<T> definition) {
    return CellExtractor.extractPojo(definition);
  }

  @Override
  public <T extends Comparable<T>> BuildableComparableOptionalFunction<Record<?>, T> extractComparable(CellDefinition<T> definition) {
    return CellExtractor.extractComparable(definition);
  }

  @Override
  public BuildableStringOptionalFunction<Record<?>> extractString(CellDefinition<String> definition) {
    return CellExtractor.extractString(definition);
  }

  @Override
  public <T> Collector<T, ?, Long> countingCollector() {
    return CountingCollector.counting();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T, K, A, D> Collector<T, ?, Map<K, D>> groupingByCollector(
          Function<? super T, ? extends K> classifier, Collector<? super T, A, D> downstream) {

    if (classifier instanceof IntrinsicFunction && downstream instanceof IntrinsicCollector) {
      return new DefaultGroupingCollector<>(
              (IntrinsicFunction<? super T, K>) classifier,
              (IntrinsicCollector<? super T, A, D>) downstream
      );
    } else {
      return FunctionProvider.super.groupingByCollector(classifier, downstream);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T, K, A, D> Collector<T, ?, ConcurrentMap<K, D>> groupingByConcurrentCollector(
          Function<? super T, ? extends K> classifier, Collector<? super T, A, D> downstream) {

    if (classifier instanceof IntrinsicFunction && downstream instanceof IntrinsicCollector) {
      return new ConcurrentGroupingCollector<>(
              (IntrinsicFunction<? super T, K>) classifier,
              (IntrinsicCollector<? super T, A, D>) downstream
      );
    } else {
      return FunctionProvider.super.groupingByConcurrentCollector(classifier, downstream);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T, D, A> Collector<T, ?, Map<Boolean, D>> partitioningCollector(
          Predicate<? super T> predicate, Collector<? super T, A, D> downstream) {

    if (predicate instanceof IntrinsicPredicate && downstream instanceof IntrinsicCollector) {
      return new PartitioningCollector<>(
              (IntrinsicPredicate<? super T>) predicate,
              (IntrinsicCollector<? super T, A, D>) downstream
      );
    } else {
      return FunctionProvider.super.partitioningCollector(predicate, downstream);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T, U, A, R> Collector<T, ?, R> mappingCollector(Function<? super T, ? extends U> mapper, Collector<? super U, A, R> downstream) {
    if (mapper instanceof IntrinsicFunction && downstream instanceof IntrinsicCollector) {
      return new MappingCollector<>(
              (IntrinsicFunction<? super T, ? extends U>) mapper,
              (IntrinsicCollector<? super U, A, R>) downstream
      );
    } else {
      return FunctionProvider.super.mappingCollector(mapper, downstream);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T, A, R> Collector<T, A, R> filteringCollector(Predicate<? super T> predicate, Collector<T, A, R> downstream) {
    if (predicate instanceof IntrinsicPredicate && downstream instanceof IntrinsicCollector) {
      return new FilteringCollector<>(
              (IntrinsicPredicate<? super T>) predicate,
              (IntrinsicCollector<T, A, R>) downstream
      );
    } else {
      return FunctionProvider.super.filteringCollector(predicate, downstream);
    }
  }

  @Override
  public <T> Collector<T, ?, IntSummaryStatistics> summarizingIntCollector(ToIntFunction<? super T> mapper) {
    if (mapper instanceof IntrinsicToIntFunction<?>) {
      return new SummarizingIntCollector<>((IntrinsicToIntFunction<? super T>) mapper);
    } else {
      return FunctionProvider.super.summarizingIntCollector(mapper);
    }
  }

  @Override
  public <T> Collector<T, ?, LongSummaryStatistics> summarizingLongCollector(ToLongFunction<? super T> mapper) {
    if (mapper instanceof IntrinsicToLongFunction<?>) {
      return new SummarizingLongCollector<>((IntrinsicToLongFunction<? super T>) mapper);
    } else {
      return FunctionProvider.super.summarizingLongCollector(mapper);
    }
  }

  @Override
  public <T> Collector<T, ?, DoubleSummaryStatistics> summarizingDoubleCollector(ToDoubleFunction<? super T> mapper) {
    if (mapper instanceof IntrinsicToDoubleFunction<?>) {
      return new SummarizingDoubleCollector<>((IntrinsicToDoubleFunction<? super T>) mapper);
    } else {
      return FunctionProvider.super.summarizingDoubleCollector(mapper);
    }
  }

  @Override
  public <K extends Comparable<K>> BuildableComparableFunction<Record<K>, K> recordKeyFunction() {
    return new RecordKey<>();
  }

  @Override
  public <T> Function<T, T> identityFunction() {
    return new IdentityFunction<>();
  }

  @Override
  public <T, U, R> BiFunction<T, U, R> inputMapper(Function<T, R> inputMappingFunction) {
    if (inputMappingFunction instanceof IntrinsicFunction<?, ?>) {
      return new InputMapper<>((IntrinsicFunction<T, R>) inputMappingFunction);
    } else {
      return FunctionProvider.super.inputMapper(inputMappingFunction);
    }
  }

  @Override
  public <T, U, R> BiFunction<T, U, R> outputMapper(Function<U, R> outputMappingFunction) {
    if (outputMappingFunction instanceof IntrinsicFunction<?, ?>) {
      return new OutputMapper<>((IntrinsicFunction<U, R>) outputMappingFunction);
    } else {
      return FunctionProvider.super.outputMapper(outputMappingFunction);
    }
  }

  public <T> BuildableFunction<Tuple<T, ?>, T> tupleFirst() {
    return TupleIntrinsic.first();
  }

  @Override
  public <U> BuildableFunction<Tuple<?, U>, U> tupleSecond() {
    return TupleIntrinsic.second();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Consumer<T> log(String message, List<Function<? super T, ?>> mappers) {
    if (mappers.stream().allMatch(i -> i instanceof IntrinsicFunction)) {
      return new IntrinsicLogger<>(message, (List<IntrinsicFunction<? super T, ?>>) (List) mappers);
    } else {
      return FunctionProvider.super.log(message, mappers);
    }
  }
}
