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
import com.terracottatech.store.StoreRuntimeException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.StreamSupport.stream;

/**
 * Not part of the public API
 */
interface FunctionProvider {

  default BuildablePredicate<Record<?>> cellDefinitionExists(CellDefinition<?> cellDefinition) {
    return r -> r.get(cellDefinition).isPresent();
  }

  default <K extends Comparable<K>> UpdateOperation<K> installUpdateOperation(Iterable<Cell<?>> cells) {
    return r -> cells;
  }

  default <K extends Comparable<K>, T> UpdateOperation.CellUpdateOperation<K,T> writeUpdateOperation(Cell<T> cell) {
    return writeUpdateOperation(cell.definition(), r -> cell.value());
  }

  default <K extends Comparable<K>, T> UpdateOperation.CellUpdateOperation<K,T> writeUpdateOperation(CellDefinition<T> definition, Function<Record<?>, T> value) {
    return new UpdateOperation.CellUpdateOperation<K, T>() {
      @Override
      public CellDefinition<T> definition() {
        return definition;
      }

      @Override
      public Function<Record<?>, Optional<Cell<T>>> cell() {
        return value.andThen(definition()::newCell).andThen(Optional::of);
      }
    };
  }

  default <K extends Comparable<K>> UpdateOperation.CellUpdateOperation<K,Integer> writeUpdateOperation(CellDefinition<Integer> definition, ToIntFunction<Record<?>> value) {
    return writeUpdateOperation(definition, (Function<Record<?>, Integer>) value::applyAsInt);
  }

  default <K extends Comparable<K>> UpdateOperation.CellUpdateOperation<K,Long> writeUpdateOperation(CellDefinition<Long> definition, ToLongFunction<Record<?>> value) {
    return writeUpdateOperation(definition, (Function<Record<?>, Long>) value::applyAsLong);
  }

  default <K extends Comparable<K>> UpdateOperation.CellUpdateOperation<K,Double> writeUpdateOperation(CellDefinition<Double> definition, ToDoubleFunction<Record<?>> value) {
    return writeUpdateOperation(definition, (Function<Record<?>, Double>) value::applyAsDouble);
  }

  default <K extends Comparable<K>> UpdateOperation.CellUpdateOperation<K,Boolean> writeUpdateOperation(CellDefinition<Boolean> definition, Predicate<Record<?>> value) {
    return writeUpdateOperation(definition, (Function<Record<?>, Boolean>) value::test);
  }

  default <K extends Comparable<K>, T> UpdateOperation.CellUpdateOperation<K,T> removeUpdateOperation(CellDefinition<T> definition) {
    return new UpdateOperation.CellUpdateOperation<K, T>() {
      @Override
      public CellDefinition<T> definition() {
        return definition;
      }

      @Override
      public Function<Record<?>, Optional<Cell<T>>> cell() {
        return r -> empty();
      }
    };
  }

  default <K extends Comparable<K>> UpdateOperation<K> allOfUpdateOperation(List<UpdateOperation.CellUpdateOperation<?,?>> transforms) {
    Collection<CellDefinition<?>> remove = transforms.stream().map(UpdateOperation.CellUpdateOperation::definition).collect(toList());

    return r -> concat(
            stream(r.spliterator(), false).filter(c -> !remove.contains(c.definition())),
            transforms.stream().map(UpdateOperation.CellUpdateOperation::cell).map(f -> f.apply(r)).filter(Optional::isPresent).map(Optional::get)
    ).collect(toList());
  }

  default <T> BuildableFunction<Record<?>, T> valueOr(CellDefinition<T> definition, T otherwise) {
    if (otherwise == null) {
      return r -> r.get(definition).get();
    } else {
      return r -> r.get(definition).orElse(otherwise);
    }
  }

  default <T extends Comparable<T>> BuildableComparableFunction<Record<?>, T> comparableValueOr(ComparableCellDefinition<T> definition, T otherwise) {
    if (otherwise == null) {
      return r -> r.get(definition).get();
    } else {
      return r -> r.get(definition).orElse(otherwise);
    }
  }

  default BuildableToIntFunction<Record<?>> intValueOr(IntCellDefinition definition, Integer otherwise) {
    if (otherwise == null) {
      return r -> r.get(definition).get();
    } else {
      return r -> r.get(definition).orElse(otherwise);
    }
  }

  default BuildableToLongFunction<Record<?>> longValueOr(LongCellDefinition definition, Long otherwise) {
    if (otherwise == null) {
      return r -> r.get(definition).get();
    } else {
      return r -> r.get(definition).orElse(otherwise);
    }
  }

  default BuildableToDoubleFunction<Record<?>> doubleValueOr(DoubleCellDefinition definition, Double otherwise) {
    if (otherwise == null) {
      return r -> r.get(definition).get();
    } else {
      return r -> r.get(definition).orElse(otherwise);
    }
  }

  default BuildableStringFunction<Record<?>> stringValueOr(StringCellDefinition definition, String otherwise) {
    if (otherwise == null) {
      return r -> r.get(definition).get();
    } else {
      return r -> r.get(definition).orElse(otherwise);
    }
  }

  default <T> BuildableOptionalFunction<Record<?>, T> extractPojo(CellDefinition<T> definition) {
    return r -> r.get(definition);
  }

  default <T extends Comparable<T>> BuildableComparableOptionalFunction<Record<?>, T> extractComparable(CellDefinition<T> definition) {
    return r -> r.get(definition);
  }

  default BuildableStringOptionalFunction<Record<?>> extractString(CellDefinition<String> definition) {
    return r -> r.get(definition);
  }

  default <T> Collector<T, ?, Long> countingCollector() {
    return Collectors.counting();
  }

  default <T, K, A, D> Collector<T, ?, Map<K, D>> groupingByCollector(
          Function<? super T, ? extends K> classifier, Collector<? super T, A, D> downstream) {
    return Collectors.groupingBy(classifier, downstream);
  }

  default <T, K, A, D> Collector<T, ?, ConcurrentMap<K, D>> groupingByConcurrentCollector(
          Function<? super T, ? extends K> classifier, Collector<? super T, A, D> downstream) {
    return Collectors.groupingByConcurrent(classifier, downstream);
  }

  default <T, D, A> Collector<T, ?, Map<Boolean, D>> partitioningCollector(
          Predicate<? super T> predicate, Collector<? super T, A, D> downstream) {
    return Collectors.partitioningBy(predicate, downstream);
  }

  default <T, U, A, R> Collector<T, ?, R> mappingCollector(Function<? super T, ? extends U> mapper, Collector<? super U, A, R> downstream) {
    return Collectors.mapping(mapper, downstream);
  }

  default <T, A, R> Collector<T, A, R> filteringCollector(Predicate<? super T> predicate, Collector<T, A, R> downstream) {
    //to be replaced with Collectors.filteringCollector in Java 9+.
    BiConsumer<A, T> accumulator = downstream.accumulator();
    return Collector.of(
            downstream.supplier(),
            (state, t) -> {
              if (predicate.test(t))
                accumulator.accept(state, t);
            },
            downstream.combiner(),
            downstream.finisher(),
            downstream.characteristics().toArray(new Collector.Characteristics[0]));
  }

  default <T> Collector<T,?,IntSummaryStatistics> summarizingIntCollector(ToIntFunction<? super T> mapper) {
    return Collectors.summarizingInt(mapper);
  }

  default <T> Collector<T,?,LongSummaryStatistics> summarizingLongCollector(ToLongFunction<? super T> mapper) {
    return Collectors.summarizingLong(mapper);
  }

  default <T> Collector<T,?,DoubleSummaryStatistics> summarizingDoubleCollector(ToDoubleFunction<? super T> mapper) {
    return Collectors.summarizingDouble(mapper);
  }

  default <K extends Comparable<K>> BuildableComparableFunction<Record<K>, K> recordKeyFunction() {
    return Record::getKey;
  }

  default <T> Function<T, T> identityFunction() {
    return a -> a;
  }

  default <T, U, R> BiFunction<T, U, R> inputMapper(Function<T, R> inputMappingFunction) {
    return (a, b) -> inputMappingFunction.apply(a);
  }

  default <T, U, R> BiFunction<T, U, R> outputMapper(Function<U, R> outputMappingFunction) {
    return (a, b) -> outputMappingFunction.apply(b);
  }

  default <T> BuildableFunction<Tuple<T, ?>, T> tupleFirst() {
    return Tuple::getFirst;
  }

  default <U> BuildableFunction<Tuple<?, U>, U> tupleSecond() {
    return Tuple::getSecond;
  }

  @SuppressWarnings("unchecked")
  default <T> Consumer<T> log(String message, List<Function<? super T, ?>> mappers) {
    return t -> {
      Logger logger = LoggerFactory.getLogger("StreamLogger");
      Object args[] = new Object[mappers.size()];

      int i = 0;
      for (Function<? super T, ?> mapper : mappers) {
        args[i++] = String.valueOf(mapper.apply(t));
      }

      logger.info(message, args);
    };
  }

  static FunctionProvider getFunctionProvider() {
    List<FunctionProvider> providers = new ArrayList<>();
    ServiceLoader<FunctionProvider> serviceLoader = ServiceLoader.load(FunctionProvider.class, FunctionProvider.class.getClassLoader());
    for (FunctionProvider provider : serviceLoader) {
      providers.add(provider);
    }

    if (providers.isEmpty()) {
      throw new StoreRuntimeException("No FunctionProvider implementation available. Check that the FunctionProvider implementation jar is in the classpath.");
    }

    if (providers.size() > 1) {
      throw new StoreRuntimeException("More than one FunctionProvider implementation found.");
    }

    return providers.get(0);
  }
}
