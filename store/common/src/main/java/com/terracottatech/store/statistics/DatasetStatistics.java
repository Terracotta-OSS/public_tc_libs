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
package com.terracottatech.store.statistics;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.stream.RecordStream;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.derived.OperationResultFilter;
import org.terracotta.statistics.derived.latency.DefaultLatencyHistogramStatistic;
import org.terracotta.statistics.observer.OperationObserver;

import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.terracottatech.store.statistics.DatasetOutcomes.AddOutcome;
import static com.terracottatech.store.statistics.DatasetOutcomes.DeleteOutcome;
import static com.terracottatech.store.statistics.DatasetOutcomes.GetOutcome;
import static com.terracottatech.store.statistics.DatasetOutcomes.StreamOutcome;
import static com.terracottatech.store.statistics.DatasetOutcomes.UpdateOutcome;
import static org.terracotta.statistics.StatisticBuilder.operation;

/**
 * Contains statistics for a given {@link Dataset} instance.
 */
public class DatasetStatistics {

  /**
   * Use to give a unique ID to each dataset instance (for a given classloader)
   */
  private static final AtomicLong idGenerator = new AtomicLong(0);

  private final Map<Class<? extends Enum<? extends DatasetOutcomes>>, OperationStatistic<? extends Enum<? extends DatasetOutcomes>>> stats = new HashMap<>(5);
  private final Map<String, ValueStatistic<?>> derivedStatistics = new HashMap<>(1);
  private final String instanceName;
  private final String datasetName;

  /**
   * Construct with the dataset name and its context. The final dataset <b>instance</b> will be derived from it but each
   * dataset instance has its own name. The context is any object that will allow to find our stats in the statistics
   * framework.
   *
   * @param datasetName name of the dataset
   * @param dataset     dataset for which are the statistics
   */
  public DatasetStatistics(String datasetName, Dataset<?> dataset) {
    this.instanceName = datasetName + "-" + idGenerator.incrementAndGet();
    this.datasetName = datasetName;

    addStatistics(GetOutcome.class, dataset);
    addStatistics(AddOutcome.class, dataset);
    addStatistics(UpdateOutcome.class, dataset);
    addStatistics(DeleteOutcome.class, dataset);
    addStatistics(StreamOutcome.class, dataset);

    addLatencyStatistic("Dataset:GetLatency", GetOutcome.class);
    addLatencyStatistic("Dataset:AddLatency", AddOutcome.class);
    addLatencyStatistic("Dataset:UpdateLatency", UpdateOutcome.class);
    addLatencyStatistic("Dataset:DeleteLatency", DeleteOutcome.class);
  }

  private <T extends Enum<T> & DatasetOutcomes> void addLatencyStatistic(String statName, Class<T> outcome) {
    DefaultLatencyHistogramStatistic histogram = new DefaultLatencyHistogramStatistic(0.63, 20, Duration.ofMinutes(1));
    getOperationStatistic(outcome).addDerivedStatistic(new OperationResultFilter<>(EnumSet.allOf(outcome), histogram));
    derivedStatistics.put(statName + "#50", histogram.medianStatistic());
    derivedStatistics.put(statName + "#95", histogram.percentileStatistic(.95));
    derivedStatistics.put(statName + "#99", histogram.percentileStatistic(.99));
    derivedStatistics.put(statName + "#100", histogram.maximumStatistic());
  }

  private <T extends Enum<T> & DatasetOutcomes> void addStatistics(Class<T> outcome, Object context) {
    OperationStatistic<T> op = (OperationStatistic<T>) operation(outcome)
        .named(outcome.getSimpleName())
        .of(context)
        .tag("clustered")
        .build();
    stats.put(outcome, op);
  }

  public Map<String, ValueStatistic<?>> getDerivedStatistics() {
    return derivedStatistics;
  }

  /**
   * Return the name of the dataset instance
   *
   * @return dataset instance name
   */
  public String getInstanceName() {
    return instanceName;
  }

  /**
   * Return the name of the underlying dataset for this instance
   *
   * @return dataset name
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * Returns all known statistics classes. These classes are enums so you can do an {@code EnumSet} on them and iterate
   * to call {@link #get(Enum)} in order to get all the known values.
   *
   * @return all known statistics name
   */
  public Set<Class<? extends Enum<? extends DatasetOutcomes>>> getKnownStatistics() {
    return stats.keySet();
  }

  /**
   * @return all known statistics outcomes
   */
  @SuppressWarnings("unchecked")
  public <T extends Enum<T> & DatasetOutcomes> Set<T> getKnownOutcomes() {
    return getKnownStatistics().stream()
        .flatMap(outcomeClass -> EnumSet.allOf((Class<T>) outcomeClass).stream())
        .collect(Collectors.toSet());
  }

  /**
   * Calls the {@code BiConsumer} for every existing statistics.
   *
   * @param consumer called for each statistics
   * @param <T>      outcome type
   */
  @SuppressWarnings("unchecked")
  public <T extends Enum<T> & DatasetOutcomes> void readAllStatistics(BiConsumer<DatasetOutcomes, Number> consumer) {
    Stream<Class<? extends Enum<? extends DatasetOutcomes>>> allCounters = getKnownStatistics().stream();
    allCounters
        .flatMap(outcomeClass -> EnumSet.allOf((Class) outcomeClass).stream())
        .forEach(outcome -> {
          T o = (T) outcome;
          consumer.accept((DatasetOutcomes) outcome, get(o));
        });
  }

  /**
   * Return the statistic for a given {@link DatasetOutcomes}.
   *
   * @param outcome outcome for which we want a value
   * @param <T>     type of the outcome
   * @return the statistics value
   */
  public <T extends Enum<T> & DatasetOutcomes> long get(T outcome) {
    @SuppressWarnings("unchecked")
    OperationStatistic<T> statistic = getOperationStatistic((Class<T>) outcome.getClass());
    if (statistic == null) {
      throw new IllegalArgumentException("Unknown statistics: " + outcome);
    }
    return statistic.count(outcome);
  }

  /**
   * Get the {@link OperationStatistic} of a given outcome.
   *
   * @param outcomeClass outcome class for which we want the statistics counter
   * @param <T>          type of the outcome
   * @return the statistics counter
   */
  @SuppressWarnings("unchecked")
  public <T extends Enum<T> & DatasetOutcomes> OperationStatistic<T> getOperationStatistic(Class<T> outcomeClass) {
    return (OperationStatistic<T>) stats.get(outcomeClass);
  }

  private static <T, K extends Enum<K> & DatasetOutcomes> Optional<T> observe(OperationObserver<K> observer, Supplier<Optional<T>> innerResult, K success, K failure, K error) {
    observer.begin();
    try {
      Optional<T> result = innerResult.get();
      observer.end(result.isPresent() ? success : failure);
      return result;
    } catch (RuntimeException e) {
      observer.end(error);
      throw e;
    }
  }

  static <T> Optional<T> observeGet(OperationObserver<GetOutcome> getObserver, Supplier<Optional<T>> innerResult) {
    return observe(getObserver, innerResult, GetOutcome.SUCCESS, GetOutcome.NOT_FOUND, GetOutcome.FAILURE);
  }

  static <T> Optional<T> observeCreate(OperationObserver<AddOutcome> addObserver, Supplier<Optional<T>> innerResult) {
    return observe(addObserver, innerResult, AddOutcome.ALREADY_EXISTS, AddOutcome.SUCCESS, AddOutcome.FAILURE);
  }

  static <T> Optional<T> observeUpdate(OperationObserver<UpdateOutcome> updateObserver, Supplier<Optional<T>> innerResult) {
    return observe(updateObserver, innerResult, UpdateOutcome.SUCCESS, UpdateOutcome.NOT_FOUND, UpdateOutcome.FAILURE);
  }

  static <T> Optional<T> observeDelete(OperationObserver<DeleteOutcome> deleteObserver, Supplier<Optional<T>> innerResult) {
    return observe(deleteObserver, innerResult, DeleteOutcome.SUCCESS, DeleteOutcome.NOT_FOUND, DeleteOutcome.FAILURE);
  }

  private static <K extends Enum<K> & DatasetOutcomes> boolean observeBoolean(OperationObserver<K> observer, Supplier<Boolean> innerResult, K success, K failure, K error) {
    observer.begin();
    try {
      boolean result = innerResult.get();
      observer.end(result ? success : failure);
      return result;
    } catch (RuntimeException e) {
      observer.end(error);
      throw e;
    }
  }

  static boolean observeBooleanCreate(OperationObserver<AddOutcome> addObserver, Supplier<Boolean> innerResult) {
    return observeBoolean(addObserver, innerResult, AddOutcome.SUCCESS, AddOutcome.ALREADY_EXISTS, AddOutcome.FAILURE);
  }

  static boolean observeBooleanUpdate(OperationObserver<UpdateOutcome> updateObserver, Supplier<Boolean> innerResult) {
    return observeBoolean(updateObserver, innerResult, UpdateOutcome.SUCCESS, UpdateOutcome.NOT_FOUND, UpdateOutcome.FAILURE);
  }

  static boolean observeBooleanDelete(OperationObserver<DeleteOutcome> deleteObserver, Supplier<Boolean> innerResult) {
    return observeBoolean(deleteObserver, innerResult, DeleteOutcome.SUCCESS, DeleteOutcome.NOT_FOUND, DeleteOutcome.FAILURE);
  }

  static <T extends RecordStream<K>, K extends Comparable<K>> T observeStream(OperationObserver<StreamOutcome> streamObserver, Supplier<T> innerResult) {
    streamObserver.begin();
    try {
      T result = innerResult.get();
      streamObserver.end(StreamOutcome.SUCCESS);
      return result;
    } catch (RuntimeException e) {
      streamObserver.end(StreamOutcome.FAILURE);
      throw e;
    }
  }

}
