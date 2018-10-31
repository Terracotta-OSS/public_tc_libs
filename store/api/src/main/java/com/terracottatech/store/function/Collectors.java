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
package com.terracottatech.store.function;

import com.terracottatech.store.internal.function.Functions;

import java.util.Collections;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

/**
 * Provides functionally transparent equivalent collectors to {@link java.util.stream.Collectors}.
 * <p>
 * The methods in this class provide {@code Collector} implementations that can be optimized by TCStore.  The equivalent
 * {@code java.util.stream.Collectors} methods are functionally-opaque and cannot be optimized by TCStore.
 */
public final class Collectors {

  /**
   * Runs a list of {@code Collector}s accepting elements of type {@code U} in parallel against the same sequence of elements.
   *
   * @param <T> the type of the input elements
   * @param collectors a list of downstream collectors which will accept the elements
   * @return a collector which provides the elements to all the downstream collectors
   */
  @SafeVarargs @SuppressWarnings("varargs")
  public static <T> Collector<T, List<Object>, List<Object>> composite(Collector<T, ?, ?> ... collectors) {
    return new CompositeCollector<>(asList(collectors));
  }

  /**
   * Functionally transparent version of
   * <a href="https://docs.oracle.com/javase/9/docs/api/java/util/stream/Collectors.html#filtering-java.util.function.Predicate-java.util.stream.Collector-">
   * {@code java.util.stream.Collectors.filtering(Predicate, Collector)}</a> (a method added in Java 9).
   *
   * @param <T> the type of the input elements
   * @param <A> intermediate accumulation type of the downstream collector
   * @param <R> result type of collector
   * @param predicate a predicate to be applied to the input elements
   * @param downstream a collector which will accept values that match the predicate
   * @return a collector which applies the predicate to the input elements and provides matching elements to the downstream collector
   */
  public static <T, A, R> Collector<T, A, R> filtering(Predicate<? super T> predicate, Collector<T, A, R> downstream) {
    return Functions.filteringCollector(predicate, downstream);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#mapping(Function, Collector)}
   *
   * @param <T> the type of the input elements
   * @param <U> type of elements accepted by downstream collector
   * @param <A> intermediate accumulation type of the downstream collector
   * @param <R> result type of collector
   * @param mapper a function to be applied to the input elements
   * @param downstream a collector which will accept mapped values
   * @return a collector which applies the mapping function to the input elements and provides the mapped results to the downstream collector
   */
  public static <T, U, A, R> Collector<T, ?, R> mapping(Function<? super T, ? extends U> mapper, Collector<? super U, A, R> downstream) {
    return Functions.mappingCollector(mapper, downstream);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#counting()}
   *
   * @param <T> the type of the input elements
   * @return a {@code Collector} that counts the input elements
   */
  public static <T> Collector<T, ?, Long> counting() {
    return Functions.countingCollector();
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#minBy(Comparator)}
   *
   * @param <T> the type of the input elements
   * @param comparator a {@code Comparator} for comparing elements
   * @return a {@code Collector} that produces the minimal value
   */
  public static <T> Collector<T, ?, Optional<T>> minBy(Comparator<? super T> comparator) {
    return java.util.stream.Collectors.minBy(comparator);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#maxBy(Comparator)}
   *
   * @param <T> the type of the input elements
   * @param comparator a {@code Comparator} for comparing elements
   * @return a {@code Collector} that produces the maximal value
   */
  public static <T> Collector<T, ?, Optional<T>> maxBy(Comparator<? super T> comparator) {
    return java.util.stream.Collectors.maxBy(comparator);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#summingInt(ToIntFunction)}
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @return a {@code Collector} that produces the sum of a derived property
   */
  public static <T> Collector<T, ?, Integer> summingInt(ToIntFunction<? super T> mapper) {
    return java.util.stream.Collectors.summingInt(mapper);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#summingLong(ToLongFunction)}
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @return a {@code Collector} that produces the sum of a derived property
   */
  public static <T> Collector<T, ?, Long> summingLong(ToLongFunction<? super T> mapper) {
    return java.util.stream.Collectors.summingLong(mapper);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#summingDouble(ToDoubleFunction)}
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @return a {@code Collector} that produces the sum of a derived property
   */
  public static <T> Collector<T, ?, Double> summingDouble(ToDoubleFunction<? super T> mapper) {
    return java.util.stream.Collectors.summingDouble(mapper);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#averagingInt(ToIntFunction)}
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @return a {@code Collector} that produces the mean of a derived property
   */
  public static <T> Collector<T, ?, Double> averagingInt(ToIntFunction<? super T> mapper) {
    return java.util.stream.Collectors.averagingInt(mapper);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#averagingLong(ToLongFunction)}
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @return a {@code Collector} that produces the mean of a derived property
   */
  public static <T> Collector<T, ?, Double> averagingLong(ToLongFunction<? super T> mapper) {
    return java.util.stream.Collectors.averagingLong(mapper);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#averagingDouble(ToDoubleFunction)}
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @return a {@code Collector} that produces the mean of a derived property
   */
  public static <T> Collector<T, ?, Double> averagingDouble(ToDoubleFunction<? super T> mapper) {
    return java.util.stream.Collectors.averagingDouble(mapper);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#groupingBy(Function, Collector)}
   *
   * @param <T> the type of the input elements
   * @param <K> the type of the keys
   * @param <A> the intermediate accumulation type of the downstream collector
   * @param <D> the result type of the downstream reduction
   * @param classifier a classifier function mapping input elements to keys
   * @param downstream a {@code Collector} implementing the downstream reduction
   * @return a {@code Collector} implementing the cascaded group-by operation
   */
  public static <T, K, A, D> Collector<T, ?, Map<K, D>> groupingBy(Function<? super T, ? extends K> classifier, Collector<? super T, A, D> downstream) {
    return Functions.groupingByCollector(classifier, downstream);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#groupingByConcurrent(Function, Collector)}
   *
   * @param <T> the type of the input elements
   * @param <K> the type of the keys
   * @param <A> the intermediate accumulation type of the downstream collector
   * @param <D> the result type of the downstream reduction
   * @param classifier a classifier function mapping input elements to keys
   * @param downstream a {@code Collector} implementing the downstream reduction
   * @return a concurrent, unordered {@code Collector} implementing the cascaded group-by operation
   */
  public static <T, K, A, D> Collector<T, ?, ConcurrentMap<K, D>> groupingByConcurrent(Function<? super T, ? extends K> classifier, Collector<? super T, A, D> downstream) {
    return Functions.groupingByConcurrentCollector(classifier, downstream);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#partitioningBy(Predicate, Collector)}
   *
   * @param <T> the type of the input elements
   * @param <A> the intermediate accumulation type of the downstream collector
   * @param <D> the result type of the downstream reduction
   * @param predicate a predicate used for classifying input elements
   * @param downstream a {@code Collector} implementing the downstream reduction
   * @return a {@code Collector} implementing the cascaded partitioning operation
   */
  public static <T, D, A> Collector<T, ?, Map<Boolean, D>> partitioningBy(Predicate<? super T> predicate, Collector<? super T, A, D> downstream) {
    return Functions.partitioningCollector(predicate, downstream);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#summarizingInt(ToIntFunction)}
   *
   * @param <T> the type of the input elements
   * @param mapper a mapping function to apply to each element
   * @return a {@code Collector} implementing the summary-statistics reduction
   */
  public static <T> Collector<T, ?, IntSummaryStatistics> summarizingInt(ToIntFunction<? super T> mapper) {
    return Functions.summarizingIntCollector(mapper);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#summarizingLong(ToLongFunction)}
   *
   * @param <T> the type of the input elements
   * @param mapper a mapping function to apply to each element
   * @return a {@code Collector} implementing the summary-statistics reduction
   */
  public static <T> Collector<T, ?, LongSummaryStatistics> summarizingLong(ToLongFunction<? super T> mapper) {
    return Functions.summarizingLongCollector(mapper);
  }

  /**
   * Functionally transparent version of {@link java.util.stream.Collectors#summarizingDouble(ToDoubleFunction)}
   *
   * @param <T> the type of the input elements
   * @param mapper a mapping function to apply to each element
   * @return a {@code Collector} implementing the summary-statistics reduction
   */
  public static <T> Collector<T, ?, DoubleSummaryStatistics> summarizingDouble(ToDoubleFunction<? super T> mapper) {
    return Functions.summarizingDoubleCollector(mapper);
  }

  /**
   * Returns a {@code Collector} that produces the statistical variance of an integer-valued function applied to the
   * input elements.
   * <p>
   * This method is deprecated in favor of {@link #varianceOfInt(ToIntFunction, VarianceType)} as this method is
   * overloaded in an ambiguous manner by the other type variant variance collector methods.
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @param type the requested variance type
   * @return a {@code Collector} that produces the variance of a derived property
   * @deprecated in favor of {@link #varianceOfInt(ToIntFunction, VarianceType)}
   */
  @Deprecated @SuppressWarnings("overloads")
  public static <T> Collector<T, ?, Optional<Double>> varianceOf(ToIntFunction<T> mapper, VarianceType type) {
    return varianceOfInt(mapper, type);
  }

  /**
   * Returns a {@code Collector} that produces the statistical variance of an integer-valued function applied to the
   * input elements.
   * <p>
   * Population or sample variance can be selected using the {@code type} argument.
   * If the variance type is {@link VarianceType#POPULATION} and no elements are present, the result is an empty optional.
   * If the variance type is {@link VarianceType#SAMPLE} and less than two elements are present, then result is an empty optional.
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @param type the requested variance type
   * @return a {@code Collector} that produces the variance of a derived property
   */
  public static <T> Collector<T, ?, Optional<Double>> varianceOfInt(ToIntFunction<T> mapper, VarianceType type) {
    return varianceCollector(mapper::applyAsInt, type);
  }

  /**
   * Returns a {@code Collector} that produces the statistical variance of a long-valued function applied to the
   * input elements.
   * <p>
   * This method is deprecated in favor of {@link #varianceOfLong(ToLongFunction, VarianceType)} as this method is
   * overloaded in an ambiguous manner by the other type variant variance collector methods.
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @param type the requested variance type
   * @return a {@code Collector} that produces the variance of a derived property
   * @deprecated in favor of {@link #varianceOfLong(ToLongFunction, VarianceType)}
   */
  @Deprecated @SuppressWarnings("overloads")
  public static <T> Collector<T, ?, Optional<Double>> varianceOf(ToLongFunction<T> mapper, VarianceType type) {
    return varianceOfLong(mapper, type);
  }

  /**
   * Returns a {@code Collector} that produces the statistical variance of a long-valued function applied to the
   * input elements.
   * <p>
   * Population or sample variance can be selected using the {@code type} argument.
   * If the variance type is {@link VarianceType#POPULATION} and no elements are present, the result is an empty optional.
   * If the variance type is {@link VarianceType#SAMPLE} and less than two elements are present, then result is an empty optional.
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @param type the requested variance type
   * @return a {@code Collector} that produces the variance of a derived property
   */
  public static <T> Collector<T, ?, Optional<Double>> varianceOfLong(ToLongFunction<T> mapper, VarianceType type) {
    return varianceCollector(mapper::applyAsLong, type);
  }

  /**
   * Returns a {@code Collector} that produces the statistical variance of a double-valued function applied to the
   * input elements.
   * <p>
   * This method is deprecated in favor of {@link #varianceOfDouble(ToDoubleFunction, VarianceType)} as this method is
   * overloaded in an ambiguous manner by the other type variant variance collector methods.
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @param type the requested variance type
   * @return a {@code Collector} that produces the variance of a derived property
   * @deprecated in favor of {@link #varianceOfDouble(ToDoubleFunction, VarianceType)}
   */
  @Deprecated @SuppressWarnings("overloads")
  public static <T> Collector<T, ?, Optional<Double>> varianceOf(ToDoubleFunction<T> mapper, VarianceType type) {
    return varianceOfDouble(mapper, type);
  }

  /**
   * Returns a {@code Collector} that produces the statistical variance of a double-valued function applied to the
   * input elements.
   * <p>
   * Population or sample variance can be selected using the {@code type} argument.
   * If the variance type is {@link VarianceType#POPULATION} and no elements are present, the result is an empty optional.
   * If the variance type is {@link VarianceType#SAMPLE} and less than two elements are present, then result is an empty optional.
   *
   * @param <T> the type of the input elements
   * @param mapper a function extracting the property to be summed
   * @param type the requested variance type
   * @return a {@code Collector} that produces the variance of a derived property
   */
  public static <T> Collector<T, ?, Optional<Double>> varianceOfDouble(ToDoubleFunction<T> mapper, VarianceType type) {
    return varianceCollector(mapper, type);
  }

  /**
   * The set of variance statistic types.
   */
  public enum VarianceType {
    /**
     * The population variance, (aka the biased sample variance).
     * <p>
     * Mathematically: `sigma^2=1/N sum_(i=1)^N (x_i - mu)`
     */
    POPULATION {
      @Override
      protected Optional<Double> finish(VarianceStatistic statistic) {
        if (statistic.count < 1) {
          return Optional.empty();
        } else {
          return Optional.of(statistic.m2 / statistic.count);
        }
      }
    },

    /**
     * The sample variance, an unbiased estimator for the population variance (utilizing Bessel's correction).
     * <p>
     * Mathematically: `sigma^2=1/(N-1) sum_(i=1)^N (x_i - bar x)`
     */
    SAMPLE {
      @Override
      protected Optional<Double> finish(VarianceStatistic statistic) {
        if (statistic.count < 2) {
          return Optional.empty();
        } else {
          return Optional.of(statistic.m2 / (statistic.count - 1));
        }
      }
    };

    protected abstract Optional<Double> finish(VarianceStatistic statistic);
  }

  /*
   * See: Numerically Stable, Single-Pass, Parallel Statistics Algorithms
   *      J Bennett, R Grout, P Pébay, D Roe, D Thompson, 2008
   *      http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.214.8508&rep=rep1&type=pdf
   */
  private static <T> Collector<T, VarianceStatistic, Optional<Double>> varianceCollector(ToDoubleFunction<T> function, VarianceType type) {
    return Collector.of(VarianceStatistic::new,
            (r, t) -> r.accept(function.applyAsDouble(t)),
            VarianceStatistic::combine,
            type::finish);
  }

  private static class VarianceStatistic implements DoubleConsumer {

    long count = 0L;
    double m1 = 0D;
    double m2 = 0D;

    @Override
    public void accept(double value) {
      if (count == 0L) {
        count = 1L;
        m1 = value;
        m2 = 0d;
      } else {
        final long lastN = count;
        final long n = lastN + 1;
        final double lastM1 = m1;
        final double lastM2 = m2;

        final double delta = value - lastM1;
        final double delta_n = delta / n;
        final double newM1 = lastM1 + delta_n;
        final double newM2 = lastM2 + lastN * (delta * delta_n);

        count = n;
        m1 = newM1;
        m2 = newM2;
      }
    }

    public VarianceStatistic combine(VarianceStatistic other) {
      if (count == 0L) {
        return other;
      } else if (other.count == 0L) {
        return this;
      } else {
        final long n1 = count;
        final double m1_1 = m1;
        final double m2_1 = m2;

        final long n2 = other.count;
        final double m1_2 = other.m1;
        final double m2_2 = other.m2;

        final long n = n1 + n2;
        final double delta = m1_2 - m1_1;
        final double delta2 = delta * delta;
        final double newM1 = (n1 * m1_1 + n2 * m1_2) / n;
        final double newM2 = m2_1 + m2_2 + delta2 * n1 * n2 / n;

        this.count = n;
        this.m1 = newM1;
        this.m2 = newM2;
        return this;
      }
    }
  }

  private static class CompositeCollector<T> implements Collector<T, List<Object>, List<Object>> {

    private final List<Collector<T, Object, Object>> collectors;

    @SuppressWarnings("unchecked")
    private CompositeCollector(List<Collector<T, ?, ?>> collectors) {
      this.collectors = (List) collectors;
    }

    @Override
    public Supplier<List<Object>> supplier() {
      return () -> collectors.stream().map(c -> c.supplier().get()).collect(toList());
    }

    @Override
    public BiConsumer<List<Object>, T> accumulator() {
      return (la, t) -> zip(collectors.stream().map(Collector::accumulator), la.stream(), (c, a) -> {
        c.accept(a, t);
        return null;
      }).count();
    }

    @Override
    public BinaryOperator<List<Object>> combiner() {
      return (la, lb) -> zip(zip(
              collectors.stream().map(Collector::combiner), la.stream(), (c, a) -> (Function<Object, Object>) ((Object b) -> c.apply(a, b))),
              lb.stream(), Function::apply).collect(toList());
    }

    @Override
    public Function<List<Object>, List<Object>> finisher() {
      return la -> zip(collectors.stream().map(Collector::finisher), la.stream(), Function::apply)
              .collect(collectingAndThen(toList(), Collections::unmodifiableList));
    }

    @Override
    public Set<Characteristics> characteristics() {
      return emptySet();
    }
  }

  private static <A, B, C> Stream<C> zip(Stream<? extends A> a, Stream<? extends B> b,
                                       BiFunction<? super A, ? super B, ? extends C> zipper) {
    Spliterator<? extends A> aSpliterator = a.spliterator();
    Spliterator<? extends B> bSpliterator = b.spliterator();

    Spliterator<C> zippedSpliterator = new Spliterator<C>() {
      @Override
      public boolean tryAdvance(Consumer<? super C> action) {
        return aSpliterator.tryAdvance(a -> {
          if (!bSpliterator.tryAdvance(b -> action.accept(zipper.apply(a, b)))) {
            throw new IllegalArgumentException("Mismatched stream lengths");
          }
        }) || bSpliterator.tryAdvance(b -> {
          throw new IllegalArgumentException("Mismatched stream lengths");
        });
      }

      @Override
      public Spliterator<C> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return aSpliterator.estimateSize();
      }

      @Override
      public int characteristics() {
        return 0;
      }
    };

    return StreamSupport.stream(zippedSpliterator, false);

  }
}