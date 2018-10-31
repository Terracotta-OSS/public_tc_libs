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

package com.terracottatech.store.intrinsics;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.BytesCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.function.BuildableFunction;
import com.terracottatech.store.function.Collectors;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import com.terracottatech.store.intrinsics.impl.CountingCollector;
import com.terracottatech.store.intrinsics.impl.IntrinsicLogger;
import com.terracottatech.store.stream.RecordStream;
import com.terracottatech.tool.MethodAnalyser;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;

import static com.terracottatech.store.definition.CellDefinition.defineString;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/*
 * This test attempts to auto-magically explore the entire DSL API call tree, and then makes assertions regarding
 * the set of "non-intrinsic" returns.  Each test here is asserting on what call-paths return non-intrinsic results.
 * If you make a change that adds or removes a "non-intrinsic" thing this test should fail (assuming coverage is
 * complete).  Fixing is as simple as removing or adding lines corresponding with the things you believe you should
 * have affected.
 *
 * Updates to the exception lists in this class need to be reflected in the user-facing documentation -- the
 * DSL functions which do not produce portable implementations are identified in the documentation describing
 * portability.
 */
public class FunctionsCompletenessTest {

  private static final List<?> PARAMETERS = asList(
          "foo",
          42,
          Boolean.TRUE,
          BuildableFunction.identity(),

          CellDefinition.defineInt("bar"),
          CellDefinition.defineInt("bar").value().isGreaterThan(0),
          CellDefinition.defineInt("bar").intValueOr(0),
          CellDefinition.defineInt("bar").valueOr(0).asComparator(),

          CellDefinition.defineLong("bar").longValueOr(0),
          CellDefinition.defineDouble("bar").doubleValueOr(0),
          Cell.cell("bar", 0),
          new UpdateOperation.CellUpdateOperation<?, ?>[]{UpdateOperation.write(Cell.cell("bar", 0))},
          Collections.singletonList(UpdateOperation.write(Cell.cell("bar", 0))),
          new Cell<?>[]{Cell.cell("bar", 0)},
          singletonList(Cell.cell("bar", 0)),
          new Function<?, ?>[]{new IdentityFunction<>()},
          new IntrinsicLogger<String>("", emptyList()),
          CountingCollector.counting(),
          new Collector<?, ?, ?>[]{CountingCollector.counting()},
          AlwaysTrue.alwaysTrue(),
          Collectors.VarianceType.SAMPLE
  );

  private static MethodAnalyser analyser = new MethodAnalyser(PARAMETERS);

  @Test
  public void testBooleanCellDefinition() {
    assertThat(testCompleteness(CellDefinition.defineBool("foo"), BoolCellDefinition.class), containsInAnyOrder(
            "BoolCellDefinition.exists().boxed().andThen(Function)",
            "BoolCellDefinition.exists().boxed().compose(Function)",
            "BoolCellDefinition.exists().boxed().asComparator().thenComparing(Comparator)",
            "BoolCellDefinition.exists().boxed().asComparator().thenComparing(Function)",
            "BoolCellDefinition.exists().boxed().asComparator().thenComparingDouble(ToDoubleFunction)",
            "BoolCellDefinition.exists().boxed().asComparator().thenComparingInt(ToIntFunction)",
            "BoolCellDefinition.exists().boxed().asComparator().thenComparingLong(ToLongFunction)",
            "BoolCellDefinition.exists().boxed().asComparator().thenComparing(Function, Comparator)",
            "BoolCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Comparator)",
            "BoolCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function)",
            "BoolCellDefinition.exists().boxed().asComparator().reversed().thenComparingDouble(ToDoubleFunction)",
            "BoolCellDefinition.exists().boxed().asComparator().reversed().thenComparingInt(ToIntFunction)",
            "BoolCellDefinition.exists().boxed().asComparator().reversed().thenComparingLong(ToLongFunction)",
            "BoolCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function, Comparator)",
            "BoolCellDefinition.value().andThen(Function)",
            "BoolCellDefinition.value().compose(Function)",
            "BoolCellDefinition.valueOrFail().andThen(Function)",
            "BoolCellDefinition.valueOrFail().compose(Function)"
    ));
  }

  @Test
  public void testCharCellDefinition() {
    assertThat(testCompleteness(CellDefinition.defineChar("foo"), CharCellDefinition.class), containsInAnyOrder(
            "CharCellDefinition.exists().boxed().andThen(Function)",
            "CharCellDefinition.exists().boxed().compose(Function)",
            "CharCellDefinition.exists().boxed().asComparator().thenComparing(Comparator)",
            "CharCellDefinition.exists().boxed().asComparator().thenComparing(Function)",
            "CharCellDefinition.exists().boxed().asComparator().thenComparingDouble(ToDoubleFunction)",
            "CharCellDefinition.exists().boxed().asComparator().thenComparingInt(ToIntFunction)",
            "CharCellDefinition.exists().boxed().asComparator().thenComparingLong(ToLongFunction)",
            "CharCellDefinition.exists().boxed().asComparator().thenComparing(Function, Comparator)",
            "CharCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Comparator)",
            "CharCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function)",
            "CharCellDefinition.exists().boxed().asComparator().reversed().thenComparingDouble(ToDoubleFunction)",
            "CharCellDefinition.exists().boxed().asComparator().reversed().thenComparingInt(ToIntFunction)",
            "CharCellDefinition.exists().boxed().asComparator().reversed().thenComparingLong(ToLongFunction)",
            "CharCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function, Comparator)",
            "CharCellDefinition.value().andThen(Function)",
            "CharCellDefinition.value().compose(Function)",
            "CharCellDefinition.valueOrFail().andThen(Function)",
            "CharCellDefinition.valueOrFail().compose(Function)"
    ));
  }

  @Test
  public void testIntegerCellDefinition() {
    assertThat(testCompleteness(CellDefinition.defineInt("foo"), IntCellDefinition.class), containsInAnyOrder(
            "IntCellDefinition.exists().boxed().andThen(Function)",
            "IntCellDefinition.exists().boxed().asComparator().thenComparing(Comparator)",
            "IntCellDefinition.exists().boxed().asComparator().thenComparing(Function)",
            "IntCellDefinition.exists().boxed().asComparator().thenComparing(Function, Comparator)",
            "IntCellDefinition.exists().boxed().asComparator().thenComparingDouble(ToDoubleFunction)",
            "IntCellDefinition.exists().boxed().asComparator().thenComparingInt(ToIntFunction)",
            "IntCellDefinition.exists().boxed().asComparator().thenComparingLong(ToLongFunction)",
            "IntCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Comparator)",
            "IntCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function)",
            "IntCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function, Comparator)",
            "IntCellDefinition.exists().boxed().asComparator().reversed().thenComparingDouble(ToDoubleFunction)",
            "IntCellDefinition.exists().boxed().asComparator().reversed().thenComparingInt(ToIntFunction)",
            "IntCellDefinition.exists().boxed().asComparator().reversed().thenComparingLong(ToLongFunction)",
            "IntCellDefinition.exists().boxed().compose(Function)",
            "IntCellDefinition.intValueOrFail().boxed().andThen(Function)",
            "IntCellDefinition.intValueOrFail().boxed().compose(Function)",
            "IntCellDefinition.value().andThen(Function)",
            "IntCellDefinition.value().compose(Function)",
            "IntCellDefinition.valueOrFail().andThen(Function)",
            "IntCellDefinition.valueOrFail().compose(Function)"
    ));
  }

  @Test
  public void testLongCellDefinition() {
    assertThat(testCompleteness(CellDefinition.defineLong("foo"), LongCellDefinition.class), containsInAnyOrder(
            "LongCellDefinition.exists().boxed().andThen(Function)",
            "LongCellDefinition.exists().boxed().asComparator().thenComparing(Comparator)",
            "LongCellDefinition.exists().boxed().asComparator().thenComparing(Function)",
            "LongCellDefinition.exists().boxed().asComparator().thenComparing(Function, Comparator)",
            "LongCellDefinition.exists().boxed().asComparator().thenComparingDouble(ToDoubleFunction)",
            "LongCellDefinition.exists().boxed().asComparator().thenComparingInt(ToIntFunction)",
            "LongCellDefinition.exists().boxed().asComparator().thenComparingLong(ToLongFunction)",
            "LongCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Comparator)",
            "LongCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function)",
            "LongCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function, Comparator)",
            "LongCellDefinition.exists().boxed().asComparator().reversed().thenComparingDouble(ToDoubleFunction)",
            "LongCellDefinition.exists().boxed().asComparator().reversed().thenComparingInt(ToIntFunction)",
            "LongCellDefinition.exists().boxed().asComparator().reversed().thenComparingLong(ToLongFunction)",
            "LongCellDefinition.exists().boxed().compose(Function)",
            "LongCellDefinition.longValueOrFail().boxed().andThen(Function)",
            "LongCellDefinition.longValueOrFail().boxed().compose(Function)",
            "LongCellDefinition.value().andThen(Function)",
            "LongCellDefinition.value().compose(Function)",
            "LongCellDefinition.valueOrFail().andThen(Function)",
            "LongCellDefinition.valueOrFail().compose(Function)"
    ));
  }

  @Test
  public void testDoubleCellDefinition() {
    assertThat(testCompleteness(CellDefinition.defineDouble("foo"), DoubleCellDefinition.class), containsInAnyOrder(
            "DoubleCellDefinition.doubleValueOrFail().asComparator().thenComparing(Comparator)",
            "DoubleCellDefinition.doubleValueOrFail().asComparator().thenComparing(Function)",
            "DoubleCellDefinition.doubleValueOrFail().asComparator().thenComparingDouble(ToDoubleFunction)",
            "DoubleCellDefinition.doubleValueOrFail().asComparator().thenComparingInt(ToIntFunction)",
            "DoubleCellDefinition.doubleValueOrFail().asComparator().thenComparingLong(ToLongFunction)",
            "DoubleCellDefinition.doubleValueOrFail().asComparator().thenComparing(Function, Comparator)",
            "DoubleCellDefinition.doubleValueOrFail().asComparator().reversed().thenComparing(Comparator)",
            "DoubleCellDefinition.doubleValueOrFail().asComparator().reversed().thenComparing(Function)",
            "DoubleCellDefinition.doubleValueOrFail().asComparator().reversed().thenComparingDouble(ToDoubleFunction)",
            "DoubleCellDefinition.doubleValueOrFail().asComparator().reversed().thenComparingInt(ToIntFunction)",
            "DoubleCellDefinition.doubleValueOrFail().asComparator().reversed().thenComparingLong(ToLongFunction)",
            "DoubleCellDefinition.doubleValueOrFail().asComparator().reversed().thenComparing(Function, Comparator)",
            "DoubleCellDefinition.doubleValueOrFail().boxed().andThen(Function)",
            "DoubleCellDefinition.doubleValueOrFail().boxed().compose(Function)",
            "DoubleCellDefinition.doubleValueOrFail().boxed().is(Object).boxed().andThen(Function)",
            "DoubleCellDefinition.doubleValueOrFail().boxed().is(Object).boxed().compose(Function)",
            "DoubleCellDefinition.value().andThen(Function)",
            "DoubleCellDefinition.value().compose(Function)",
            "DoubleCellDefinition.valueOrFail().andThen(Function)",
            "DoubleCellDefinition.valueOrFail().compose(Function)"
    ));
  }

  @Test
  public void testStringCellDefinition() {
    assertThat(testCompleteness(defineString("foo"), StringCellDefinition.class), containsInAnyOrder(
            "StringCellDefinition.exists().boxed().andThen(Function)",
            "StringCellDefinition.exists().boxed().compose(Function)",
            "StringCellDefinition.exists().boxed().asComparator().thenComparing(Comparator)",
            "StringCellDefinition.exists().boxed().asComparator().thenComparing(Function)",
            "StringCellDefinition.exists().boxed().asComparator().thenComparingDouble(ToDoubleFunction)",
            "StringCellDefinition.exists().boxed().asComparator().thenComparingInt(ToIntFunction)",
            "StringCellDefinition.exists().boxed().asComparator().thenComparingLong(ToLongFunction)",
            "StringCellDefinition.exists().boxed().asComparator().thenComparing(Function, Comparator)",
            "StringCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Comparator)",
            "StringCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function)",
            "StringCellDefinition.exists().boxed().asComparator().reversed().thenComparingDouble(ToDoubleFunction)",
            "StringCellDefinition.exists().boxed().asComparator().reversed().thenComparingInt(ToIntFunction)",
            "StringCellDefinition.exists().boxed().asComparator().reversed().thenComparingLong(ToLongFunction)",
            "StringCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function, Comparator)",
            "StringCellDefinition.value().length().andThen(Function)",
            "StringCellDefinition.value().length().compose(Function)",
            "StringCellDefinition.value().andThen(Function)",
            "StringCellDefinition.value().compose(Function)",
            "StringCellDefinition.valueOrFail().length().boxed().andThen(Function)",
            "StringCellDefinition.valueOrFail().length().boxed().compose(Function)",
            "StringCellDefinition.valueOrFail().andThen(Function)",
            "StringCellDefinition.valueOrFail().compose(Function)"
    ));
  }

  @Test
  public void testBytesCellDefinition() {
    assertThat(testCompleteness(CellDefinition.defineBytes("foo"), BytesCellDefinition.class), containsInAnyOrder(
            "BytesCellDefinition.exists().boxed().andThen(Function)",
            "BytesCellDefinition.exists().boxed().asComparator().thenComparing(Comparator)",
            "BytesCellDefinition.exists().boxed().asComparator().thenComparing(Function)",
            "BytesCellDefinition.exists().boxed().asComparator().thenComparing(Function, Comparator)",
            "BytesCellDefinition.exists().boxed().asComparator().thenComparingDouble(ToDoubleFunction)",
            "BytesCellDefinition.exists().boxed().asComparator().thenComparingInt(ToIntFunction)",
            "BytesCellDefinition.exists().boxed().asComparator().thenComparingLong(ToLongFunction)",
            "BytesCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Comparator)",
            "BytesCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function)",
            "BytesCellDefinition.exists().boxed().asComparator().reversed().thenComparing(Function, Comparator)",
            "BytesCellDefinition.exists().boxed().asComparator().reversed().thenComparingDouble(ToDoubleFunction)",
            "BytesCellDefinition.exists().boxed().asComparator().reversed().thenComparingInt(ToIntFunction)",
            "BytesCellDefinition.exists().boxed().asComparator().reversed().thenComparingLong(ToLongFunction)",
            "BytesCellDefinition.exists().boxed().compose(Function)",
            "BytesCellDefinition.value().andThen(Function)",
            "BytesCellDefinition.value().compose(Function)",
            "BytesCellDefinition.valueOrFail().andThen(Function)",
            "BytesCellDefinition.valueOrFail().compose(Function)"
    ));
  }

  @Test
  public void testUpdateOperationStatics() {
    assertThat(testCompleteness(UpdateOperation.class), containsInAnyOrder(
            "UpdateOperation.custom(Function)",
            "UpdateOperation.remove(CellDefinition).cell()",
            "UpdateOperation.write(Cell).cell()"
    ));
  }

  @Test
  public void testBuildableFunctionStatics() {
    assertThat(testCompleteness(BuildableFunction.class), is(Matchers.empty()));
  }

  @Test
  public void testRecordStatics() {
    assertThat(testCompleteness(Record.class), containsInAnyOrder(
            "Record.keyFunction().andThen(Function)",
            "Record.keyFunction().compose(Function)",
            "Record.keyFunction().asComparator().thenComparing(Comparator)",
            "Record.keyFunction().asComparator().thenComparing(Function, Comparator)",
            "Record.keyFunction().asComparator().thenComparing(Function)",
            "Record.keyFunction().asComparator().thenComparingDouble(ToDoubleFunction)",
            "Record.keyFunction().asComparator().thenComparingInt(ToIntFunction)",
            "Record.keyFunction().asComparator().thenComparingLong(ToLongFunction)",
            "Record.keyFunction().asComparator().reversed().thenComparing(Comparator)",
            "Record.keyFunction().asComparator().reversed().thenComparing(Function, Comparator)",
            "Record.keyFunction().asComparator().reversed().thenComparing(Function)",
            "Record.keyFunction().asComparator().reversed().thenComparingDouble(ToDoubleFunction)",
            "Record.keyFunction().asComparator().reversed().thenComparingInt(ToIntFunction)",
            "Record.keyFunction().asComparator().reversed().thenComparingLong(ToLongFunction)",
            "Record.keyFunction().is(Object).boxed().andThen(Function)",
            "Record.keyFunction().is(Object).boxed().compose(Function)"
    ));
  }

  @Test
  public void testRecordStreamStatics() {
    assertThat(testCompleteness(RecordStream.class), containsInAnyOrder(
        "RecordStream.log(String, Function[]).andThen(Consumer)"
    ));
  }

  @Test
  public void testCollectorsStatics() {
    assertThat(testCompleteness(Collectors.class), containsInAnyOrder(
            "Collectors.averagingDouble(ToDoubleFunction)",
            "Collectors.averagingInt(ToIntFunction)",
            "Collectors.averagingLong(ToLongFunction)",
            "Collectors.composite(Collector[])",
            "Collectors.maxBy(Comparator)",
            "Collectors.minBy(Comparator)",
            "Collectors.summarizingDouble(ToDoubleFunction).accumulator()",
            "Collectors.summarizingDouble(ToDoubleFunction).combiner()",
            "Collectors.summarizingDouble(ToDoubleFunction).finisher()",
            "Collectors.summarizingDouble(ToDoubleFunction).supplier()",
            "Collectors.summarizingInt(ToIntFunction).accumulator()",
            "Collectors.summarizingInt(ToIntFunction).combiner()",
            "Collectors.summarizingInt(ToIntFunction).finisher()",
            "Collectors.summarizingInt(ToIntFunction).supplier()",
            "Collectors.summarizingLong(ToLongFunction).accumulator()",
            "Collectors.summarizingLong(ToLongFunction).combiner()",
            "Collectors.summarizingLong(ToLongFunction).finisher()",
            "Collectors.summarizingLong(ToLongFunction).supplier()",
            "Collectors.summingDouble(ToDoubleFunction)",
            "Collectors.summingInt(ToIntFunction)",
            "Collectors.summingLong(ToLongFunction)",
            "Collectors.filtering(Predicate, Collector).accumulator()",
            "Collectors.filtering(Predicate, Collector).combiner()",
            "Collectors.filtering(Predicate, Collector).finisher()",
            "Collectors.filtering(Predicate, Collector).supplier()",
            "Collectors.groupingBy(Function, Collector).accumulator()",
            "Collectors.groupingBy(Function, Collector).combiner()",
            "Collectors.groupingBy(Function, Collector).finisher()",
            "Collectors.groupingBy(Function, Collector).supplier()",
            "Collectors.groupingByConcurrent(Function, Collector).accumulator()",
            "Collectors.groupingByConcurrent(Function, Collector).combiner()",
            "Collectors.groupingByConcurrent(Function, Collector).finisher()",
            "Collectors.groupingByConcurrent(Function, Collector).supplier()",
            "Collectors.mapping(Function, Collector).accumulator()",
            "Collectors.mapping(Function, Collector).combiner()",
            "Collectors.mapping(Function, Collector).finisher()",
            "Collectors.mapping(Function, Collector).supplier()",
            "Collectors.partitioningBy(Predicate, Collector).accumulator()",
            "Collectors.partitioningBy(Predicate, Collector).combiner()",
            "Collectors.partitioningBy(Predicate, Collector).finisher()",
            "Collectors.partitioningBy(Predicate, Collector).supplier()",
            "Collectors.varianceOf(ToDoubleFunction, VarianceType)",
            "Collectors.varianceOf(ToIntFunction, VarianceType)",
            "Collectors.varianceOf(ToLongFunction, VarianceType)",
            "Collectors.varianceOfDouble(ToDoubleFunction, VarianceType)",
            "Collectors.varianceOfInt(ToIntFunction, VarianceType)",
            "Collectors.varianceOfLong(ToLongFunction, VarianceType)"
    ));
  }

  private List<String> testCompleteness(Class<?> holder) {
    return testCompleteness(null, holder);
  }

  private <T> List<String> testCompleteness(T foo, Class<T> as) {
    return recurse(foo, as, new HashSet<>(), as.getSimpleName());
  }

  private List<String> recurse(Object foo, Class<?> as, Set<Class<?>> inspected, String entryPath) {
    List<String> badPaths = new ArrayList<>();
    analyser.mapMethods(foo, as).entrySet().stream()
        .sorted(comparing(Map.Entry::getKey,
            comparing(Method::getParameterCount).thenComparing(MethodAnalyser::methodString)))
        .forEachOrdered(e -> {
              Object returnValue = e.getValue();
              if (analyser.ignoreParameter(returnValue) && !inspected.contains(returnValue.getClass())) {
                String path = entryPath + "." + MethodAnalyser.methodString(e.getKey());
                if (returnValue instanceof Intrinsic) {
                  try {
                    if (Object.class.equals(returnValue.getClass().getMethod("toString").getDeclaringClass())) {
                      throw new IllegalStateException("Missing toString implemention from " + returnValue.getClass());
                    } else {
                      inspected.add(returnValue.getClass());
                      badPaths.addAll(recurse(returnValue, e.getKey().getReturnType(), inspected, path));
                    }
                  } catch (NoSuchMethodException nsme) {
                    throw new AssertionError(nsme);
                  }
                } else {
                  badPaths.add(path);
                }
              }
            }
        );
    return badPaths;
  }
}
