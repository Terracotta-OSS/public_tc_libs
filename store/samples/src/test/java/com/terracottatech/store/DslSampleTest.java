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
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.function.BuildableComparableOptionalFunction;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.function.BuildableToIntFunction;
import org.junit.Test;

import java.util.Comparator;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import static com.terracottatech.store.definition.CellDefinition.defineBool;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static java.util.Spliterators.emptySpliterator;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Fragments of this test class are used to provide code samples for the user documentation. Please take care when editing.
 */
@SuppressWarnings("unchecked")
public class DslSampleTest {

  @Test
  public void booleanCellSamples() {
    //tag::booleanCellSamples[]
    BoolCellDefinition definition = defineBool("cell");
    Predicate<Record<?>> exists = definition.exists(); // <1>
    Predicate<Record<?>> isTrue = definition.isTrue(); // <2>
    //end::booleanCellSamples[]

    Record<?> emptyRecord = mock(Record.class);
    assertThat(exists.test(emptyRecord), is(false));
    assertThat(isTrue.test(emptyRecord), is(false));

    Record<?> falseRecord = mock(Record.class);
    when(falseRecord.get(definition)).thenReturn(Optional.of(false));
    assertThat(exists.test(falseRecord), is(true));
    assertThat(isTrue.test(falseRecord), is(false));

    Record<?> trueRecord = mock(Record.class);
    when(trueRecord.get(definition)).thenReturn(Optional.of(true));
    assertThat(exists.test(trueRecord), is(true));
    assertThat(isTrue.test(trueRecord), is(true));
  }

  @Test
  public void stringCellSamples() {
    //tag::stringCellSamples[]
    StringCellDefinition definition = defineString("cell");
    BuildableComparableOptionalFunction<Record<?>, String> value = definition.value(); // <1>

    Predicate<Record<?>> isFoo = value.is("foo"); // <2>
    Predicate<Record<?>> isAfterBar = value.isGreaterThan("bar"); // <3>
    //end::stringCellSamples[]

    Record<?> emptyRecord = mock(Record.class);
    assertThat(value.apply(emptyRecord), is(Optional.empty()));
    assertThat(isFoo.test(emptyRecord), is(false));
    assertThat(isAfterBar.test(emptyRecord), is(false));

    Record<?> fooRecord = mock(Record.class);
    when(fooRecord.get(definition)).thenReturn(Optional.of("foo"));
    assertThat(value.apply(fooRecord), is(Optional.of("foo")));
    assertThat(isFoo.test(fooRecord), is(true));
    assertThat(isAfterBar.test(fooRecord), is(true));

    Record<?> bazRecord = mock(Record.class);
    when(bazRecord.get(definition)).thenReturn(Optional.of("baz"));
    assertThat(value.apply(bazRecord), is(Optional.of("baz")));
    assertThat(isFoo.test(bazRecord), is(false));
    assertThat(isAfterBar.test(bazRecord), is(true));
  }

  @Test
  public void integerCellSamples() {
    //tag::integerCellSamples[]
    IntCellDefinition definition = defineInt("cell");

    BuildableToIntFunction<Record<?>> intValue = definition.intValueOr(0);  // <1>
    BuildablePredicate<Record<?>> isGreaterThanOrEqualTo4 = intValue.isGreaterThanOrEqualTo(4);  // <2>
    ToIntFunction<Record<?>> incremented = intValue.increment(); // <3>
    Comparator<Record<?>> comparator = intValue.asComparator(); // <4>
    //end::integerCellSamples[]

    Record<?> emptyRecord = mock(Record.class);
    assertThat(intValue.applyAsInt(emptyRecord), is(0));
    assertThat(isGreaterThanOrEqualTo4.test(emptyRecord), is(false));
    assertThat(incremented.applyAsInt(emptyRecord), is(1));

    Record<?> zeroRecord = mock(Record.class);
    when(zeroRecord.get(definition)).thenReturn(Optional.of(0));
    assertThat(intValue.applyAsInt(zeroRecord), is(0));
    assertThat(isGreaterThanOrEqualTo4.test(zeroRecord), is(false));
    assertThat(incremented.applyAsInt(zeroRecord), is(1));

    Record<?> fourRecord = mock(Record.class);
    when(fourRecord.get(definition)).thenReturn(Optional.of(4));
    assertThat(intValue.applyAsInt(fourRecord), is(4));
    assertThat(isGreaterThanOrEqualTo4.test(fourRecord), is(true));
    assertThat(incremented.applyAsInt(fourRecord), is(5));

    assertThat(comparator.compare(emptyRecord, emptyRecord), is(0));
    assertThat(comparator.compare(emptyRecord, zeroRecord), is(0));
    assertThat(comparator.compare(emptyRecord, fourRecord), lessThan(0));

    assertThat(comparator.compare(zeroRecord, emptyRecord), is(0));
    assertThat(comparator.compare(zeroRecord, zeroRecord), is(0));
    assertThat(comparator.compare(zeroRecord, fourRecord), lessThan(0));

    assertThat(comparator.compare(fourRecord, emptyRecord), greaterThan(0));
    assertThat(comparator.compare(fourRecord, zeroRecord), greaterThan(0));
    assertThat(comparator.compare(fourRecord, fourRecord), is(0));
  }

  @Test
  public void updateOperationSamples() {
    //tag::updateOperationSamples[]
    IntCellDefinition defnA = defineInt("cell-a");
    IntCellDefinition defnB = defineInt("cell-b");

    UpdateOperation<Long> install = UpdateOperation.install(defnA.newCell(42), defnB.newCell(42)); // <1>

    UpdateOperation.CellUpdateOperation<Long, Integer> write = UpdateOperation.write(defnA).value(42); // <2>

    UpdateOperation.CellUpdateOperation<Long, Integer> increment = UpdateOperation.write(defnA).intResultOf(defnA.intValueOr(0).increment()); // <3>

    UpdateOperation.CellUpdateOperation<Long, Integer> copy = UpdateOperation.write(defnB).intResultOf(defnA.intValueOr(42));

    UpdateOperation<Long> aggregate = UpdateOperation.allOf(increment, copy); // <4>
    //end::updateOperationSamples[]

    Record<Long> emptyRecord = mock(Record.class);
    when(emptyRecord.spliterator()).thenReturn(emptySpliterator());
    assertThat(install.apply(emptyRecord), containsInAnyOrder(defnA.newCell(42), defnB.newCell(42)));
    assertThat(write.apply(emptyRecord), containsInAnyOrder(defnA.newCell(42)));
    assertThat(increment.apply(emptyRecord), containsInAnyOrder(defnA.newCell(1)));
    assertThat(copy.apply(emptyRecord), containsInAnyOrder(defnB.newCell(42)));
    assertThat(aggregate.apply(emptyRecord), containsInAnyOrder(defnA.newCell(1), defnB.newCell(42)));

    Record<Long> defnARecord = mock(Record.class);
    when(defnARecord.spliterator()).thenAnswer(invocation -> Stream.of(defnA.newCell(10)).spliterator());
    when(defnARecord.get(defnA)).thenReturn(Optional.of(10));
    assertThat(install.apply(defnARecord), containsInAnyOrder(defnA.newCell(42), defnB.newCell(42)));
    assertThat(write.apply(defnARecord), containsInAnyOrder(defnA.newCell(42)));
    assertThat(increment.apply(defnARecord), containsInAnyOrder(defnA.newCell(11)));
    assertThat(copy.apply(defnARecord), containsInAnyOrder(defnA.newCell(10), defnB.newCell(10)));
    assertThat(aggregate.apply(defnARecord), containsInAnyOrder(defnA.newCell(11), defnB.newCell(10)));

    Record<Long> defnBRecord = mock(Record.class);
    when(defnBRecord.spliterator()).thenAnswer(invocation -> Stream.of(defnB.newCell(10)).spliterator());
    when(defnBRecord.get(defnB)).thenReturn(Optional.of(10));
    assertThat(install.apply(defnBRecord), containsInAnyOrder(defnA.newCell(42), defnB.newCell(42)));
    assertThat(write.apply(defnBRecord), containsInAnyOrder(defnA.newCell(42), defnB.newCell(10)));
    assertThat(increment.apply(defnBRecord), containsInAnyOrder(defnA.newCell(1), defnB.newCell(10)));
    assertThat(copy.apply(defnBRecord), containsInAnyOrder(defnB.newCell(42)));
    assertThat(aggregate.apply(defnBRecord), containsInAnyOrder(defnA.newCell(1), defnB.newCell(42)));

    Record<Long> defnAdefnBRecord = mock(Record.class);
    when(defnAdefnBRecord.spliterator()).thenAnswer(invocation -> Stream.of(defnA.newCell(10), defnB.newCell(10)).spliterator());
    when(defnAdefnBRecord.get(defnA)).thenReturn(Optional.of(10));
    when(defnAdefnBRecord.get(defnB)).thenReturn(Optional.of(10));
    assertThat(install.apply(defnAdefnBRecord), containsInAnyOrder(defnA.newCell(42), defnB.newCell(42)));
    assertThat(write.apply(defnAdefnBRecord), containsInAnyOrder(defnA.newCell(42), defnB.newCell(10)));
    assertThat(increment.apply(defnAdefnBRecord), containsInAnyOrder(defnA.newCell(11), defnB.newCell(10)));
    assertThat(copy.apply(defnAdefnBRecord), containsInAnyOrder(defnA.newCell(10), defnB.newCell(10)));
    assertThat(aggregate.apply(defnAdefnBRecord), containsInAnyOrder(defnA.newCell(11), defnB.newCell(10)));
  }

  @Test
  public void updateOutputSamples() {
    IntCellDefinition defnA = defineInt("cell-a");

    //tag::updateOutputSamples[]
    BiFunction<Record<?>, Record<?>, Integer> inputBiFunction = UpdateOperation.input(defnA.valueOr(42)); // <1>
    BiFunction<Record<?>, Record<?>, Integer> outputBiFunction = UpdateOperation.output(defnA.valueOr(42)); // <2>

    Function<Tuple<Record<?>, ?>, Integer> inputTupleFunction = Tuple.<Record<?>>first().andThen(defnA.valueOr(42)); // <3>
    Function<Tuple<?, Record<?>>, Integer> outputTupleFunction = Tuple.<Record<?>>second().andThen(defnA.valueOr(42)); // <4>
    //end::updateOutputSamples[]

    Record<Long> emptyRecord = mock(Record.class);
    when(emptyRecord.spliterator()).thenReturn(emptySpliterator());

    Record<Long> defnARecord = mock(Record.class);
    when(defnARecord.get(defnA)).thenReturn(Optional.of(10));

    assertThat(inputBiFunction.apply(emptyRecord, emptyRecord), is(42));
    assertThat(outputBiFunction.apply(emptyRecord, emptyRecord), is(42));
    assertThat(inputTupleFunction.apply(Tuple.of(emptyRecord, emptyRecord)), is(42));
    assertThat(outputTupleFunction.apply(Tuple.of(emptyRecord, emptyRecord)), is(42));

    assertThat(inputBiFunction.apply(emptyRecord, defnARecord), is(42));
    assertThat(outputBiFunction.apply(emptyRecord, defnARecord), is(10));
    assertThat(inputTupleFunction.apply(Tuple.of(emptyRecord, defnARecord)), is(42));
    assertThat(outputTupleFunction.apply(Tuple.of(emptyRecord, defnARecord)), is(10));

    assertThat(inputBiFunction.apply(defnARecord, emptyRecord), is(10));
    assertThat(outputBiFunction.apply(defnARecord, emptyRecord), is(42));
    assertThat(inputTupleFunction.apply(Tuple.of(defnARecord, emptyRecord)), is(10));
    assertThat(outputTupleFunction.apply(Tuple.of(defnARecord, emptyRecord)), is(42));

    assertThat(inputBiFunction.apply(defnARecord, defnARecord), is(10));
    assertThat(outputBiFunction.apply(defnARecord, defnARecord), is(10));
    assertThat(inputTupleFunction.apply(Tuple.of(defnARecord, defnARecord)), is(10));
    assertThat(outputTupleFunction.apply(Tuple.of(defnARecord, defnARecord)), is(10));
  }

  @Test
  public void keyExtractionSamples() {
    //tag::keyExtractionSamples[]
    Predicate<Record<String>> greaterThan = Record.<String>keyFunction().isGreaterThan("foo"); // <1>
    //end::keyExtractionSamples[]

    Record<String> fooRecord = mock(Record.class);
    when(fooRecord.getKey()).thenReturn("foo");
    assertThat(greaterThan.test(fooRecord), is(false));

    Record<String> fopRecord = mock(Record.class);
    when(fopRecord.getKey()).thenReturn("fop");
    assertThat(greaterThan.test(fopRecord), is(true));
  }
}
