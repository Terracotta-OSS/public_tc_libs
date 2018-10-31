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

package com.terracottatech.store.common.dataset.stream;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.Operation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * @author Clifford W. Johnson
 */
public class PipelineOperationTest {

  @Test
  public void testInstantiation() throws Exception {
    final Supplier<ArrayList<Object>> supplier = ArrayList::new;
    final BiConsumer<ArrayList<Object>, String> accumulator = ArrayList::add;
    final BiConsumer<ArrayList<Object>, ArrayList<Object>> combiner = ArrayList::addAll;

    final PipelineOperation pipelineOperation =
        TerminalOperation.COLLECT_3.newInstance(supplier, accumulator, combiner);
    assertThat(pipelineOperation, is(notNullValue()));
    assertThat(pipelineOperation.getOperation(), is(TerminalOperation.COLLECT_3));
    assertThat(pipelineOperation.getArguments(), contains(supplier, accumulator, combiner));
  }

  @Test
  public void testMismatchedArgumentCount() throws Exception {
    final Supplier<ArrayList<Object>> supplier = ArrayList::new;
    final BiConsumer<ArrayList<Object>, String> accumulator = ArrayList::add;

    try {
      TerminalOperation.COLLECT_3.newInstance(supplier, accumulator);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), startsWith(TerminalOperation.COLLECT_3.name() + " operation argument mismatch"));
    }
  }

  @Test
  public void testMismatchedArgumentClass() throws Exception {
    final Supplier<ArrayList<Object>> supplier = ArrayList::new;
    final BiConsumer<ArrayList<Object>, String> accumulator = ArrayList::add;

    try {
      TerminalOperation.COLLECT_3.newInstance(supplier, accumulator, supplier);
      fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), startsWith(TerminalOperation.COLLECT_3.name() + " operation argument mismatch"));
    }
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testSupportsReconstruction() throws Exception {
    /*
     * Known to support reconstruction.
     */
    assertTrue(IntermediateOperation.FILTER.supportsReconstruction());

    /*
     * Known to **not** support reconstruction.
     */
    assertFalse(IntermediateOperation.ON_CLOSE.supportsReconstruction());

    /*
     * Check supportsReconstruction() vs reconstruct()
     */
    for (PipelineMethod method : PipelineMethod.values()) {
      Operation op = method.getOperation();
      boolean supportsReconstruction = op.supportsReconstruction();
      Class<? extends BaseStream> streamClass = method.getStreamClass();
      List<? extends BaseStream> mocks;
      if (streamClass == BaseStream.class) {
        mocks = Arrays.asList(mock(Stream.class), mock(DoubleStream.class), mock(IntStream.class), mock(LongStream.class));
      } else {
        mocks = Collections.singletonList(mock(streamClass));
      }
      for (BaseStream baseStream : mocks) {
        try {
          op.reconstruct(baseStream, Collections.emptyList());
          if (!supportsReconstruction) {
            fail("Reconstruction supported on " + op + "; supportsReconstruction=false");
          }
        } catch (IndexOutOfBoundsException e) {
          if (!supportsReconstruction) {
            fail("Reconstruction supported on " + op + "; supportsReconstruction=false");
          }
        } catch (UnsupportedOperationException e) {
          if (supportsReconstruction) {
            throw new AssertionError("Reconstruction not supported on " + op + "; supportsReconstruction=true", e);
          }
          assertThat(e.getMessage(), allOf(containsString("Reconstruction"), containsString("not supported")));
        }
      }
    }
  }

  /**
   * This test makes an attempt an ensuring all of the Java framework pipeline operations are
   * covered by a {@link PipelineOperation.Operation}.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testOperationMapping() throws Exception {

    /*
     * Ensure the PipelineMethod enum has an entry for all of the operations declared in PipelineOperation.
     * The locally-declared operations are, of course, excluded from this check.
     */
    Set<Operation> declaredOperations = new HashSet<>();
    declaredOperations.addAll(EnumSet.allOf(PipelineOperation.IntermediateOperation.class));
    declaredOperations.addAll(EnumSet.allOf(PipelineOperation.TerminalOperation.class));
    declaredOperations.remove(IntermediateOperation.EXPLAIN);
    declaredOperations.remove(IntermediateOperation.SELF_CLOSE);
    declaredOperations.remove(IntermediateOperation.DELETE_THEN);
    declaredOperations.remove(IntermediateOperation.MUTATE_THEN);
    declaredOperations.remove(IntermediateOperation.MUTATE_THEN_INTERNAL);
    declaredOperations.remove(TerminalOperation.DELETE);
    declaredOperations.remove(TerminalOperation.MUTATE);
    declaredOperations.removeAll(
        Arrays.stream(PipelineMethod.values())
            .map(PipelineMethod::getOperation)
            .collect(Collectors.toSet()));
    assertThat("PipelineMethod enum does not have an entry for all PipelineOperation operations",
        declaredOperations, is(empty()));

    /*
     * The declared public methods that aren't pipeline operations ...
     */
    Set<Map.Entry<Class<?>, String>> excludedMethods = setOf(
        new SimpleImmutableEntry<>(BaseStream.class, "isParallel"),
        new SimpleImmutableEntry<>(BaseStream.class, "close")
    );

    /*
     * Cross-reference the methods declared in the stream classes with the PipelineMethod enum and
     * and identify any declared methods *not* covered in the PipelineMethod enum.
     */
    Map<Optional<PipelineMethod>, List<Method>> operationMap =
        Stream.<Class<?>>of(BaseStream.class, Stream.class, DoubleStream.class, IntStream.class, LongStream.class)
            .flatMap(c -> Arrays.stream(c.getMethods()))
            .filter(m -> {
              /*
               * Include only public, non-static, non-bridge, non-synthetic methods -- i.e. the instance
               * methods that can be overridden.
               */
              int modifiers = m.getModifiers();
              return Modifier.isPublic(modifiers) && !Modifier.isStatic(modifiers) && !m.isBridge() && !m.isSynthetic()
                  && !excludedMethods.contains(new SimpleImmutableEntry<Class<?>, String>(m.getDeclaringClass(), m.getName()));
            })
            .collect(Collectors.groupingBy(m -> {
              try {
                return Optional.of(PipelineMethod.from(m));
              } catch (EnumConstantNotPresentException e) {
                return Optional.<PipelineMethod>empty();
              }
            }));

    if (javaMajor() >= 9) {
      assertThat(operationMap.get(Optional.<PipelineMethod>empty()), containsInAnyOrder(
          method(Stream.class, "takeWhile", Predicate.class),
          method(Stream.class, "dropWhile", Predicate.class),
          method(DoubleStream.class, "takeWhile", DoublePredicate.class),
          method(DoubleStream.class, "dropWhile", DoublePredicate.class),
          method(IntStream.class, "takeWhile", IntPredicate.class),
          method(IntStream.class, "dropWhile", IntPredicate.class),
          method(LongStream.class, "takeWhile", LongPredicate.class),
          method(LongStream.class, "dropWhile", LongPredicate.class)
      ));
    } else {
      assertThat(operationMap.get(Optional.<PipelineMethod>empty()), is(nullValue()));
    }

    /*
     * Make a note of any PipelineOperation items *not* having a matching streams method -- probably
     * not an error but useful information for hygiene purposes.
     */
    Set<PipelineMethod> pipelineMethods = EnumSet.allOf(PipelineMethod.class);
    operationMap.keySet().forEach(o -> o.ifPresent(pipelineMethods::remove));
    if (!pipelineMethods.isEmpty()) {
      System.out.format("%s contains Operations without corresponding stream methods:%n",
          PipelineOperation.class.getSimpleName());
      for (PipelineMethod pipelineMethod : pipelineMethods) {
        System.out.format("    %s%n", pipelineMethod);
      }
    }
  }

  @SuppressWarnings("varargs")
  @SafeVarargs
  private static <K, V> Set<Map.Entry<K, V>> setOf(Map.Entry<K, V>... items) {
    return new HashSet<>(Arrays.asList(items));
  }

  private static final Map<Class<?>, Class<?>> WRAPPER_TO_PRIMITIVE;
  static {
    Map<Class<?>, Class<?>> wrapperToPrimitive = new IdentityHashMap<>();
    wrapperToPrimitive.put(Boolean.class, Boolean.TYPE);
    wrapperToPrimitive.put(Character.class, Character.TYPE);
    wrapperToPrimitive.put(Byte.class, Byte.TYPE);
    wrapperToPrimitive.put(Short.class, Short.TYPE);
    wrapperToPrimitive.put(Integer.class, Integer.TYPE);
    wrapperToPrimitive.put(Long.class, Long.TYPE);
    wrapperToPrimitive.put(Float.class, Float.TYPE);
    wrapperToPrimitive.put(Double.class, Double.TYPE);
    wrapperToPrimitive.put(Void.class, Void.TYPE);
    WRAPPER_TO_PRIMITIVE = Collections.unmodifiableMap(wrapperToPrimitive);
  }

  /**
   * Associates each {@link PipelineOperation.Operation} with it's corresponding {@code Stream} method.
   * Several methods are shared among one or more of the  {@code Stream} classes -- {@code BaseStream},
   * {@code Stream}, {@code DoubleStream}, {@code IntStream}, and {@code LongStream}; these methods are
   * represented once for each class in which they appear.  Several methods appear to be shared but
   * have different parameter types in the different classes.
   */
  @SuppressWarnings("rawtypes")
  private enum PipelineMethod {

    /*
     * Intermediate operations
     */
    AS_DOUBLE_STREAM_INT(IntermediateOperation.AS_DOUBLE_STREAM, IntStream.class, "asDoubleStream"),
    AS_DOUBLE_STREAM_LONG(IntermediateOperation.AS_DOUBLE_STREAM, LongStream.class, "asDoubleStream"),
    AS_LONG_STREAM(IntermediateOperation.AS_LONG_STREAM, IntStream.class, "asLongStream"),
    BOXED_DOUBLE(IntermediateOperation.BOXED, DoubleStream.class, "boxed"),
    BOXED_INT(IntermediateOperation.BOXED, IntStream.class, "boxed"),
    BOXED_LONG(IntermediateOperation.BOXED, LongStream.class, "boxed"),
    DISTINCT(IntermediateOperation.DISTINCT, Stream.class, "distinct"),
    DISTINCT_DOUBLE(IntermediateOperation.DISTINCT, DoubleStream.class, "distinct"),
    DISTINCT_INT(IntermediateOperation.DISTINCT, IntStream.class, "distinct"),
    DISTINCT_LONG(IntermediateOperation.DISTINCT, LongStream.class, "distinct"),
    DOUBLE_FILTER(IntermediateOperation.DOUBLE_FILTER, DoubleStream.class, "filter"),
    DOUBLE_FLAT_MAP(IntermediateOperation.DOUBLE_FLAT_MAP, DoubleStream.class, "flatMap"),
    DOUBLE_MAP(IntermediateOperation.DOUBLE_MAP, DoubleStream.class, "map"),
    DOUBLE_MAP_TO_INT(IntermediateOperation.DOUBLE_MAP_TO_INT, DoubleStream.class, "mapToInt"),
    DOUBLE_MAP_TO_LONG(IntermediateOperation.DOUBLE_MAP_TO_LONG, DoubleStream.class, "mapToLong"),
    DOUBLE_MAP_TO_OBJ(IntermediateOperation.DOUBLE_MAP_TO_OBJ, DoubleStream.class, "mapToObj"),
    DOUBLE_PEEK(IntermediateOperation.DOUBLE_PEEK, DoubleStream.class, "peek"),
    FILTER(IntermediateOperation.FILTER, Stream.class, "filter"),
    FLAT_MAP(IntermediateOperation.FLAT_MAP, Stream.class, "flatMap"),
    FLAT_MAP_TO_DOUBLE(IntermediateOperation.FLAT_MAP_TO_DOUBLE, Stream.class, "flatMapToDouble"),
    FLAT_MAP_TO_INT(IntermediateOperation.FLAT_MAP_TO_INT, Stream.class, "flatMapToInt"),
    FLAT_MAP_TO_LONG(IntermediateOperation.FLAT_MAP_TO_LONG, Stream.class, "flatMapToLong"),
    INT_FILTER(IntermediateOperation.INT_FILTER, IntStream.class, "filter"),
    INT_FLAT_MAP(IntermediateOperation.INT_FLAT_MAP, IntStream.class, "flatMap"),
    INT_MAP(IntermediateOperation.INT_MAP, IntStream.class, "map"),
    INT_MAP_TO_DOUBLE(IntermediateOperation.INT_MAP_TO_DOUBLE, IntStream.class, "mapToDouble"),
    INT_MAP_TO_LONG(IntermediateOperation.INT_MAP_TO_LONG, IntStream.class, "mapToLong"),
    INT_MAP_TO_OBJ(IntermediateOperation.INT_MAP_TO_OBJ, IntStream.class, "mapToObj"),
    INT_PEEK(IntermediateOperation.INT_PEEK, IntStream.class, "peek"),
    LIMIT(IntermediateOperation.LIMIT, Stream.class, "limit"),
    LIMIT_DOUBLE(IntermediateOperation.LIMIT, DoubleStream.class, "limit"),
    LIMIT_INT(IntermediateOperation.LIMIT, IntStream.class, "limit"),
    LIMIT_LONG(IntermediateOperation.LIMIT, LongStream.class, "limit"),
    LONG_FILTER(IntermediateOperation.LONG_FILTER, LongStream.class, "filter"),
    LONG_FLAT_MAP(IntermediateOperation.LONG_FLAT_MAP, LongStream.class, "flatMap"),
    LONG_MAP(IntermediateOperation.LONG_MAP, LongStream.class, "map"),
    LONG_MAP_TO_DOUBLE(IntermediateOperation.LONG_MAP_TO_DOUBLE, LongStream.class, "mapToDouble"),
    LONG_MAP_TO_INT(IntermediateOperation.LONG_MAP_TO_INT, LongStream.class, "mapToInt"),
    LONG_MAP_TO_OBJ(IntermediateOperation.LONG_MAP_TO_OBJ, LongStream.class, "mapToObj"),
    LONG_PEEK(IntermediateOperation.LONG_PEEK, LongStream.class, "peek"),
    MAP(IntermediateOperation.MAP, Stream.class, "map"),
    MAP_TO_DOUBLE(IntermediateOperation.MAP_TO_DOUBLE, Stream.class, "mapToDouble"),
    MAP_TO_INT(IntermediateOperation.MAP_TO_INT, Stream.class, "mapToInt"),
    MAP_TO_LONG(IntermediateOperation.MAP_TO_LONG, Stream.class, "mapToLong"),
    ON_CLOSE(IntermediateOperation.ON_CLOSE, BaseStream.class, "onClose"),
    PARALLEL(IntermediateOperation.PARALLEL, BaseStream.class, "parallel"),
    PARALLEL_DOUBLE(IntermediateOperation.PARALLEL, DoubleStream.class, "parallel"),
    PARALLEL_INT(IntermediateOperation.PARALLEL, IntStream.class, "parallel"),
    PARALLEL_LONG(IntermediateOperation.PARALLEL, LongStream.class, "parallel"),
    PEEK(IntermediateOperation.PEEK, Stream.class, "peek"),
    SEQUENTIAL(IntermediateOperation.SEQUENTIAL, BaseStream.class, "sequential"),
    SEQUENTIAL_DOUBLE(IntermediateOperation.SEQUENTIAL, DoubleStream.class, "sequential"),
    SEQUENTIAL_INT(IntermediateOperation.SEQUENTIAL, IntStream.class, "sequential"),
    SEQUENTIAL_LONG(IntermediateOperation.SEQUENTIAL, LongStream.class, "sequential"),
    SKIP(IntermediateOperation.SKIP, Stream.class, "skip"),
    SKIP_DOUBLE(IntermediateOperation.SKIP, DoubleStream.class, "skip"),
    SKIP_INT(IntermediateOperation.SKIP, IntStream.class, "skip"),
    SKIP_LONG(IntermediateOperation.SKIP, LongStream.class, "skip"),
    SORTED_0(IntermediateOperation.SORTED_0, Stream.class, "sorted"),
    SORTED_0_DOUBLE(IntermediateOperation.SORTED_0, DoubleStream.class, "sorted"),
    SORTED_0_INT(IntermediateOperation.SORTED_0, IntStream.class, "sorted"),
    SORTED_0_LONG(IntermediateOperation.SORTED_0, LongStream.class, "sorted"),
    SORTED_1(IntermediateOperation.SORTED_1, Stream.class, "sorted"),
    UNORDERED(IntermediateOperation.UNORDERED, BaseStream.class, "unordered"),

    /*
     * Terminal operations
     */
    ALL_MATCH(TerminalOperation.ALL_MATCH, Stream.class, "allMatch"),
    ANY_MATCH(TerminalOperation.ANY_MATCH, Stream.class, "anyMatch"),
    AVERAGE_DOUBLE(TerminalOperation.AVERAGE, DoubleStream.class, "average"),
    AVERAGE_INT(TerminalOperation.AVERAGE, IntStream.class, "average"),
    AVERAGE_LONG(TerminalOperation.AVERAGE, LongStream.class, "average"),
    COLLECT_1(TerminalOperation.COLLECT_1, Stream.class, "collect"),
    COLLECT_3(TerminalOperation.COLLECT_3, Stream.class, "collect"),
    COUNT(TerminalOperation.COUNT, Stream.class, "count"),
    COUNT_DOUBLE(TerminalOperation.COUNT, DoubleStream.class, "count"),
    COUNT_INT(TerminalOperation.COUNT, IntStream.class, "count"),
    COUNT_LONG(TerminalOperation.COUNT, LongStream.class, "count"),
    DOUBLE_ALL_MATCH(TerminalOperation.DOUBLE_ALL_MATCH, DoubleStream.class, "allMatch"),
    DOUBLE_ANY_MATCH(TerminalOperation.DOUBLE_ANY_MATCH, DoubleStream.class, "anyMatch"),
    DOUBLE_COLLECT(TerminalOperation.DOUBLE_COLLECT, DoubleStream.class, "collect"),
    DOUBLE_FOR_EACH(TerminalOperation.DOUBLE_FOR_EACH, DoubleStream.class, "forEach"),
    DOUBLE_FOR_EACH_ORDERED(TerminalOperation.DOUBLE_FOR_EACH_ORDERED, DoubleStream.class, "forEachOrdered"),
    DOUBLE_NONE_MATCH(TerminalOperation.DOUBLE_NONE_MATCH, DoubleStream.class, "noneMatch"),
    DOUBLE_REDUCE_1(TerminalOperation.DOUBLE_REDUCE_1, DoubleStream.class, "reduce"),
    DOUBLE_REDUCE_2(TerminalOperation.DOUBLE_REDUCE_2, DoubleStream.class, "reduce"),
    FIND_ANY(TerminalOperation.FIND_ANY, Stream.class, "findAny"),
    FIND_ANY_DOUBLE(TerminalOperation.FIND_ANY, DoubleStream.class, "findAny"),
    FIND_ANY_INT(TerminalOperation.FIND_ANY, IntStream.class, "findAny"),
    FIND_ANY_LONG(TerminalOperation.FIND_ANY, LongStream.class, "findAny"),
    FIND_FIRST(TerminalOperation.FIND_FIRST, Stream.class, "findFirst"),
    FIND_FIRST_DOUBLE(TerminalOperation.FIND_FIRST, DoubleStream.class, "findFirst"),
    FIND_FIRST_INT(TerminalOperation.FIND_FIRST, IntStream.class, "findFirst"),
    FIND_FIRST_LONG(TerminalOperation.FIND_FIRST, LongStream.class, "findFirst"),
    FOR_EACH(TerminalOperation.FOR_EACH, Stream.class, "forEach"),
    FOR_EACH_ORDERED(TerminalOperation.FOR_EACH_ORDERED, Stream.class, "forEachOrdered"),
    INT_ALL_MATCH(TerminalOperation.INT_ALL_MATCH, IntStream.class, "allMatch"),
    INT_ANY_MATCH(TerminalOperation.INT_ANY_MATCH, IntStream.class, "anyMatch"),
    INT_COLLECT(TerminalOperation.INT_COLLECT, IntStream.class, "collect"),
    INT_FOR_EACH(TerminalOperation.INT_FOR_EACH, IntStream.class, "forEach"),
    INT_FOR_EACH_ORDERED(TerminalOperation.INT_FOR_EACH_ORDERED, IntStream.class, "forEachOrdered"),
    INT_NONE_MATCH(TerminalOperation.INT_NONE_MATCH, IntStream.class, "noneMatch"),
    INT_REDUCE_1(TerminalOperation.INT_REDUCE_1, IntStream.class, "reduce"),
    INT_REDUCE_2(TerminalOperation.INT_REDUCE_2, IntStream.class, "reduce"),
    ITERATOR(TerminalOperation.ITERATOR, BaseStream.class, "iterator"),
    ITERATOR_DOUBLE(TerminalOperation.ITERATOR, DoubleStream.class, "iterator"),
    ITERATOR_INT(TerminalOperation.ITERATOR, IntStream.class, "iterator"),
    ITERATOR_LONG(TerminalOperation.ITERATOR, LongStream.class, "iterator"),
    LONG_ALL_MATCH(TerminalOperation.LONG_ALL_MATCH,LongStream.class, "allMatch"),
    LONG_ANY_MATCH(TerminalOperation.LONG_ANY_MATCH, LongStream.class, "anyMatch"),
    LONG_COLLECT(TerminalOperation.LONG_COLLECT, LongStream.class, "collect"),
    LONG_FOR_EACH(TerminalOperation.LONG_FOR_EACH, LongStream.class, "forEach"),
    LONG_FOR_EACH_ORDERED(TerminalOperation.LONG_FOR_EACH_ORDERED, LongStream.class, "forEachOrdered"),
    LONG_NONE_MATCH(TerminalOperation.LONG_NONE_MATCH, LongStream.class, "noneMatch"),
    LONG_REDUCE_1(TerminalOperation.LONG_REDUCE_1, LongStream.class, "reduce"),
    LONG_REDUCE_2(TerminalOperation.LONG_REDUCE_2, LongStream.class, "reduce"),
    MAX_0_DOUBLE(TerminalOperation.MAX_0, DoubleStream.class, "max"),
    MAX_0_INT(TerminalOperation.MAX_0, IntStream.class, "max"),
    MAX_0_LONG(TerminalOperation.MAX_0, LongStream.class, "max"),
    MAX_1(TerminalOperation.MAX_1, Stream.class, "max"),
    MIN_0_DOUBLE(TerminalOperation.MIN_0, DoubleStream.class, "min"),
    MIN_0_INT(TerminalOperation.MIN_0, IntStream.class, "min"),
    MIN_0_LONG(TerminalOperation.MIN_0, LongStream.class, "min"),
    MIN_1(TerminalOperation.MIN_1, Stream.class, "min"),
    NONE_MATCH(TerminalOperation.NONE_MATCH, Stream.class, "noneMatch"),
    REDUCE_1(TerminalOperation.REDUCE_1, Stream.class, "reduce"),
    REDUCE_2(TerminalOperation.REDUCE_2, Stream.class, "reduce"),
    REDUCE_3(TerminalOperation.REDUCE_3, Stream.class, "reduce"),
    SPLITERATOR(TerminalOperation.SPLITERATOR, BaseStream.class, "spliterator"),
    SPLITERATOR_DOUBLE(TerminalOperation.SPLITERATOR, DoubleStream.class, "spliterator"),
    SPLITERATOR_INT(TerminalOperation.SPLITERATOR, IntStream.class, "spliterator"),
    SPLITERATOR_LONG(TerminalOperation.SPLITERATOR, LongStream.class, "spliterator"),
    SUM_DOUBLE(TerminalOperation.SUM, DoubleStream.class, "sum"),
    SUM_INT(TerminalOperation.SUM, IntStream.class, "sum"),
    SUM_LONG(TerminalOperation.SUM, LongStream.class, "sum"),
    SUMMARY_STATISTICS_DOUBLE(TerminalOperation.SUMMARY_STATISTICS, DoubleStream.class, "summaryStatistics"),
    SUMMARY_STATISTICS_INT(TerminalOperation.SUMMARY_STATISTICS, IntStream.class, "summaryStatistics"),
    SUMMARY_STATISTICS_LONG(TerminalOperation.SUMMARY_STATISTICS, LongStream.class, "summaryStatistics"),
    TO_ARRAY_0(TerminalOperation.TO_ARRAY_0, Stream.class, "toArray"),
    TO_ARRAY_0_DOUBLE(TerminalOperation.TO_ARRAY_0, DoubleStream.class, "toArray"),
    TO_ARRAY_0_INT(TerminalOperation.TO_ARRAY_0, IntStream.class, "toArray"),
    TO_ARRAY_0_LONG(TerminalOperation.TO_ARRAY_0, LongStream.class, "toArray"),
    TO_ARRAY_1(TerminalOperation.TO_ARRAY_1, Stream.class, "toArray"),
    ;

    private final Operation operation;
    private final Class<? extends BaseStream> streamClass;
    private final Method operationMethod;

    PipelineMethod(Operation operation, Class<? extends BaseStream> streamClass, String methodName) {
      this.operation = operation;
      this.streamClass = streamClass;
      try {
        Class[] parameterTypes = operation.getArgumentTypes().stream()
            .map(c -> WRAPPER_TO_PRIMITIVE.getOrDefault(c, c))
            .toArray(Class[]::new);
        this.operationMethod = streamClass.getMethod(methodName, parameterTypes);
      } catch (NoSuchMethodException e) {
        throw new AssertionError(e);
      }
    }

    public Operation getOperation() {
      return operation;
    }

    public Class<? extends BaseStream> getStreamClass() {
      return streamClass;
    }

    public Method getOperationMethod() {
      return operationMethod;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(128);
      sb.append(name());
      sb.append("{");
      sb.append("operation=").append(operation);
      sb.append(", operationMethod=").append(operationMethod);
      sb.append('}');
      return sb.toString();
    }

    public static PipelineMethod from(Method method) {
      for (PipelineMethod value : values()) {
        if (value.getOperationMethod().equals(method)) {
          return value;
        }
      }
      throw new EnumConstantNotPresentException(PipelineMethod.class, method.toString());
    }
  }

  private static final int JAVA_MAJOR;
  static {
    Matcher matcher = Pattern.compile("([1-9][0-9]*(?:(?:\\.0)*\\.[1-9][0-9]*)*).*")
        .matcher(System.getProperty("java.version"));
    if (matcher.matches()) {
      String[] versionComponents = matcher.group(1).split("\\.");
      int major = Integer.parseInt(versionComponents[0]);
      if (major > 1) {
        JAVA_MAJOR = major;
      } else {
        JAVA_MAJOR = Integer.parseInt(versionComponents[1]);
      }
    } else {
      throw new IllegalStateException("Unexpected parse failure on java.version " + matcher);
    }
  }

  /**
   * Get the "major" Java runtime version.  The version numbering scheme for Java changes
   * between Java 1.8 and Java 9 -- Java 9 drops the leading "1" (among other changes).
   * @return the major version of the current Java runtime
   */
  protected int javaMajor() {
    return JAVA_MAJOR;
  }

  private org.hamcrest.Matcher<Method> method(Class<?> returnType, String methodName, Class<?>... arguments) {
    return new TypeSafeMatcher<Method>() {
      @Override
      protected boolean matchesSafely(Method method) {
        return returnType.equals(method.getReturnType()) && methodName.equals(method.getName()) && Arrays.equals(arguments, method.getParameterTypes());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("method ").appendValue(returnType).appendText(" ").appendText(methodName)
            .appendValueList("(", ", ", ")", arguments);
      }
    };
  }
}
