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
package com.terracottatech.store.common.messages.intrinsics;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.TestRecord;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.common.messages.stream.NonPortableTransform;
import com.terracottatech.store.common.messages.stream.inline.WaypointMarker;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.function.BuildableComparableOptionalFunction;
import com.terracottatech.store.function.BuildableFunction;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.function.BuildableToIntFunction;
import com.terracottatech.store.internal.function.Functions;
import com.terracottatech.store.intrinsics.InputMapper;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicToDoubleFunction;
import com.terracottatech.store.intrinsics.IntrinsicToIntFunction;
import com.terracottatech.store.intrinsics.IntrinsicToLongFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;
import com.terracottatech.store.intrinsics.OutputMapper;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import com.terracottatech.store.intrinsics.impl.CellValue;
import com.terracottatech.store.intrinsics.impl.ComparableComparator;
import com.terracottatech.store.intrinsics.impl.ComparisonType;
import com.terracottatech.store.intrinsics.impl.ConcurrentGroupingCollector;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.intrinsics.impl.DefaultGroupingCollector;
import com.terracottatech.store.intrinsics.impl.FilteringCollector;
import com.terracottatech.store.intrinsics.impl.IntrinsicLogger;
import com.terracottatech.store.intrinsics.impl.MappingCollector;
import com.terracottatech.store.intrinsics.impl.NonGatedComparison;
import com.terracottatech.store.intrinsics.impl.PartitioningCollector;
import com.terracottatech.store.intrinsics.impl.PredicateFunctionAdapter;
import com.terracottatech.store.intrinsics.impl.RecordEquality;
import com.terracottatech.store.intrinsics.impl.RecordKey;
import com.terracottatech.store.intrinsics.impl.RecordSameness;
import com.terracottatech.store.intrinsics.impl.ReverseComparableComparator;
import com.terracottatech.store.intrinsics.impl.StringCellExtractor;
import com.terracottatech.store.intrinsics.impl.ToDoubleFunctionAdapter;
import com.terracottatech.store.intrinsics.impl.ToIntFunctionAdapter;
import com.terracottatech.store.intrinsics.impl.ToLongFunctionAdapter;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;

import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.UpdateOperation.remove;
import static com.terracottatech.store.definition.CellDefinition.defineBool;
import static com.terracottatech.store.definition.CellDefinition.defineDouble;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.internal.function.Functions.identityFunction;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_FILTERING;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_GROUPING;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_GROUPING_CONCURRENT;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_MAPPING;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_PARTITIONING;
import static com.terracottatech.store.intrinsics.IntrinsicType.COMPARATOR;
import static com.terracottatech.store.intrinsics.IntrinsicType.CONSUMER_LOG;
import static com.terracottatech.store.intrinsics.IntrinsicType.COUNTING_COLLECTOR;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_PREDICATE_ADAPTER;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_RECORD_KEY;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_TO_DOUBLE_ADAPTER;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_TO_INT_ADAPTER;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_TO_LONG_ADAPTER;
import static com.terracottatech.store.intrinsics.IntrinsicType.IDENTITY_FUNCTION;
import static com.terracottatech.store.intrinsics.IntrinsicType.INPUT_MAPPER;
import static com.terracottatech.store.intrinsics.IntrinsicType.OUTPUT_MAPPER;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_CONTRAST;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_EQUALS;
import static com.terracottatech.store.intrinsics.IntrinsicType.REVERSE_COMPARATOR;
import static com.terracottatech.store.intrinsics.IntrinsicType.TUPLE_FIRST;
import static com.terracottatech.store.intrinsics.IntrinsicType.TUPLE_SECOND;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
@SuppressWarnings("unchecked")
public class IntrinsicCodecTest {

  private final IntrinsicCodec intrinsicCodec = new IntrinsicCodec(TestIntrinsicDescriptors.OVERRIDDEN_DESCRIPTORS);
  private final Struct myStruct = StructBuilder.newStructBuilder().struct("intrinsic", 1, intrinsicCodec.intrinsicStruct()).build();

  private Intrinsic encodeAndDecode(Object intrinsic) {
    StructEncoder<?> encoder = myStruct.encoder();
    intrinsicCodec.encodeIntrinsic(encoder.struct("intrinsic"), (Intrinsic) intrinsic);
    ByteBuffer encoded = encoder.encode();

    encoded.rewind();

    StructDecoder<?> decoder = myStruct.decoder(encoded);
    return intrinsicCodec.decodeIntrinsic(decoder.struct("intrinsic"));
  }

  @SuppressWarnings("unchecked")
  private Predicate<Record<?>> encodeAndDecodePredicate(Predicate<? extends Record<?>> predicate) {
    Intrinsic decoded = encodeAndDecode(predicate);
    assertThat(predicate, equalTo(decoded));
    assertEquals(predicate.hashCode(), decoded.hashCode());
    return (Predicate<Record<?>>) decoded;
  }

  @SuppressWarnings("unchecked")
  private <T, R> Function<T, R> encodeAndDecodeFunction(Function<T, R> function) {
    Intrinsic decoded = encodeAndDecode(function);
    assertThat(function, equalTo(decoded));
    assertEquals(function.hashCode(), decoded.hashCode());
    return (Function<T, R>) decoded;
  }

  @Test(expected = AssertionError.class)
  public void testMissingEncoderDecoderThrows() throws Exception {
    // recordEqualsPredicate has no default encoder/decoder, so no descriptor override makes IntrinsicCodec throw AssertionError
    new IntrinsicCodec(Collections.emptyMap());
  }

  @Test(expected = ClassCastException.class)
  public void testFailureToEncodeNonIntrinsic() throws Exception {
    // ClassCastException: lambda cannot be cast to Intrinsic
    encodeAndDecode((Predicate<String>) s -> false);
  }

  @Test
  public void testRecordEqualsPredicate() {
    Collection<Cell<?>> cells1 = Collections.singleton(Cell.cell("city", "Kuala Lumpur"));
    Collection<Cell<?>> cells2 = Collections.singleton(Cell.cell("name", "bob"));
    RecordEquality<?> equality = new RecordEquality<>(new TestIntrinsicDescriptors.DupeRecord<>(123L, "aaa", cells1));
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(equality);
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(123L, "aaa", cells1)), is(true));
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(123L, "aaa", cells2)), is(false));
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(1234L, "aaa", cells1)), is(true));
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(1234L, "aaa", cells2)), is(false));
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(123L, "bbb", cells1)), is(false));
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(123L, "bbb", cells2)), is(false));
  }

  @Test
  public void testRecordSamenessPredicate() {
    Collection<Cell<?>> cells1 = Collections.singleton(Cell.cell("city", "Kuala Lumpur"));
    Collection<Cell<?>> cells2 = Collections.singleton(Cell.cell("name", "bob"));
    RecordSameness<?> sameness = new RecordSameness<>(123L, "aaa");
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(sameness);
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(123L, "aaa", cells1)), is(true));
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(123L, "aaa", cells2)), is(true));
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(1234L, "aaa", cells1)), is(false));
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(1234L, "aaa", cells2)), is(false));
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(123L, "bbb", cells1)), is(false));
    assertThat(predicate.test(new TestIntrinsicDescriptors.DupeRecord<>(123L, "bbb", cells2)), is(false));
  }

  @Test
  public void testAlwaysTrueIntrinsic() {
    IntrinsicPredicate<Record<String>> alwaysTrue = AlwaysTrue.alwaysTrue();
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(alwaysTrue);
    assertThat(alwaysTrue, equalTo(predicate));
  }

  @Test
  public void testCellDefinitionExists() {
    Predicate<Record<?>> exists = Functions.cellDefinitionExists(defineInt("blah"));
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(exists);
    assertThat(predicate.test(new TestRecord<>(123L, "aaa", Arrays.asList())), is(false));
    assertThat(predicate.test(new TestRecord<>(123L, "aaa", Arrays.asList(Cell.cell("blah", 1L)))), is(false));
    assertThat(predicate.test(new TestRecord<>(123L, "aaa", Arrays.asList(Cell.cell("blah", 1)))), is(true));
  }

  @Test
  public void testInstallUpdateOperation() throws Exception {
    Intrinsic intrinsic = encodeAndDecode(Functions.installUpdateOperation(Arrays.asList(Cell.cell("name", "joe"), Cell.cell("age", 21))));
    Iterable<Cell<?>> cells = (Iterable<Cell<?>>) ((UpdateOperation) intrinsic).apply(null);
    List<Cell<?>> applied = new ArrayList<>();
    cells.forEach(applied::add);
    assertThat(applied, is(Arrays.asList(Cell.cell("name", "joe"), Cell.cell("age", 21))));
  }

  @Test
  public void testWritesUpdateOperation() throws Exception {
    assertContains(UpdateOperation.write(defineString("aString").newCell("aValue")), stringCell("aString", "aValue"));
    assertContains(UpdateOperation.write("aString", "aValue"), stringCell("aString", "aValue"));
    assertContains(UpdateOperation.write(defineString("aString")).value("aValue"), stringCell("aString", "aValue"));
    assertContains(UpdateOperation.write(defineString("aString")).resultOf(Functions.valueOrFail(defineString("anotherString"))),
        Arrays.asList(stringCell("aString", "anotherValue"), stringCell("anotherString", "anotherValue")),
        Arrays.asList(stringCell("anotherString", "anotherValue"))
    );
    assertContains(UpdateOperation.write(defineInt("anInt")).intResultOf(Functions.intValueOrFail(defineInt("anotherInt"))),
        Arrays.asList(intCell("anInt", 456), intCell("anotherInt", 456)),
        Arrays.asList(intCell("anotherInt", 456))
    );
    assertContains(UpdateOperation.write(defineLong("aLong")).longResultOf(Functions.longValueOrFail(defineLong("anotherLong"))),
        Arrays.asList(longCell("aLong", 123456L), longCell("anotherLong", 123456L)),
        Arrays.asList(longCell("anotherLong", 123456L))
    );
    assertContains(UpdateOperation.write(defineDouble("aDouble")).doubleResultOf(Functions.doubleValueOrFail(defineDouble("anotherDouble"))),
        Arrays.asList(doubleCell("aDouble", 0.5), doubleCell("anotherDouble", 0.5)),
        Arrays.asList(doubleCell("anotherDouble", 0.5))
    );
  }

  @Test
  public void testRemoveUpdateOperation() throws Exception {
    assertContains(remove(defineString("aString")),
        Arrays.asList(stringCell("anotherString", "anotherValue")),
        Arrays.asList(stringCell("aString", "anotherValue"), stringCell("anotherString", "anotherValue"))
    );
  }

  @Test
  public void testAllOfUpdateOperation() throws Exception {
    assertContains(UpdateOperation.allOf(
        remove(defineString("aString")), UpdateOperation.write(stringCell("anotherString", "anotherValue"))),
        Arrays.asList(stringCell("anotherString", "anotherValue")),
        Arrays.asList(stringCell("aString", "aValue"))
    );
  }

  @Test
  public void testAdds() {
    assertIntResult(defineInt("anInt").intValueOrFail().add(100), 110, Arrays.asList(intCell("anInt", 10)));
    assertLongResult(defineLong("aLong").longValueOrFail().add(100L), 110L, Arrays.asList(longCell("aLong", 10L)));
    assertDoubleResult(defineDouble("aDouble").doubleValueOrFail().add(.5), 55.55, Arrays.asList(doubleCell("aDouble", 55.05)));
  }

  @Test
  public void testMultiplies() {
    assertIntResult(defineInt("anInt").intValueOrFail().multiply(100), 200, Arrays.asList(intCell("anInt", 2)));
    assertLongResult(defineLong("aLong").longValueOrFail().multiply(100L), 200L, Arrays.asList(longCell("aLong", 2L)));
    assertDoubleResult(defineDouble("aDouble").doubleValueOrFail().multiply(.5), 1D, Arrays.asList(doubleCell("aDouble", 2D)));
  }

  @Test
  public void testDivides() {
    assertIntResult(defineInt("anInt").intValueOrFail().divide(2), 100, Arrays.asList(intCell("anInt", 200)));
    assertLongResult(defineLong("aLong").longValueOrFail().divide(2L), 100L, Arrays.asList(longCell("aLong", 200L)));
    assertDoubleResult(defineDouble("aDouble").doubleValueOrFail().divide(.5), 2D, Arrays.asList(doubleCell("aDouble", 1D)));
  }

  @Test
  public void testLength() {
    BuildableToIntFunction<Record<?>> length = defineString("aString").valueOrFail().length();
    assertIntResult(length, 5, Arrays.asList(stringCell("aString", "abcde")));
  }

  @Test
  public void testStartsWith() {
    BuildablePredicate<Record<?>> startsWith = defineString("value").valueOrFail().startsWith("prefix");
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(startsWith);
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("value", "nonPrefixedValue")))), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("value", "prefixedValue")))), is(true));
  }

  @Test
  public void testOptionalLength() {
    BuildableComparableOptionalFunction<Record<?>, Integer> length = defineString("aString").value().length();
    StringCellExtractor.Length intrinsic = (StringCellExtractor.Length) encodeAndDecode(length);
    assertThat(length, equalTo(intrinsic));
    assertEquals(length.hashCode(), intrinsic.hashCode());
    Optional<Integer> result = intrinsic.apply(new TestRecord<>("aKey", Arrays.asList(stringCell("aString", "abcde"))));
    assertThat(result.isPresent(), is(true));
    assertThat(result.orElse(0), is(5));
  }

  @Test
  public void testOptionalStartsWith() {
    BuildablePredicate<Record<?>> startsWith = defineString("value").value().startsWith("prefix");
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(startsWith);
    assertThat(predicate.test(new TestRecord<>(1, Collections.emptyList())), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("value", "nonPrefixedValue")))), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("value", "prefixedValue")))), is(true));
  }

  @Test
  public void testComparator() throws Exception {
    assertComparison(defineString("aString").valueOrFail().asComparator(),
        stringCell("aString", "a"),
        stringCell("aString", "b"), -1);

    assertComparison(defineInt("anInt").valueOrFail().asComparator(),
        intCell("anInt", 100),
        intCell("anInt", 10), 1);

    assertComparison(defineLong("aLong").valueOrFail().asComparator(),
        longCell("aLong", Long.MAX_VALUE),
        longCell("aLong", Long.MAX_VALUE), 0);

    assertComparison(defineDouble("aDouble").valueOrFail().asComparator(),
        doubleCell("aDouble", 1.0),
        doubleCell("aDouble", 0.0), 1);
  }

  @Test
  public void testComparatorReverse() throws Exception {
    assertComparison(defineString("aString").valueOrFail().asComparator().reversed(),
        stringCell("aString", "a"),
        stringCell("aString", "b"), 1);

    assertComparison(defineInt("anInt").valueOrFail().asComparator().reversed(),
        intCell("anInt", 100),
        intCell("anInt", 10), -1);

    assertComparison(defineLong("aLong").valueOrFail().asComparator().reversed(),
        longCell("aLong", Long.MAX_VALUE),
        longCell("aLong", Long.MAX_VALUE), 0);

    assertComparison(defineDouble("aDouble").valueOrFail().asComparator().reversed(),
        doubleCell("aDouble", 1.0),
        doubleCell("aDouble", 0.0), -1);
  }

  @Test
  public void testInstallUpdateOperationAllCellTypes() throws Exception {
    Intrinsic intrinsic = encodeAndDecode(Functions.installUpdateOperation(Arrays.asList(
        Cell.cell("string", "en français, s'il vous plaît."),
        Cell.cell("int", 21),
        Cell.cell("long", 1000L),
        Cell.cell("boolean", true),
        Cell.cell("char", '%'),
        Cell.cell("double", Double.NaN),
        Cell.cell("bytes", new byte[]{1, 2, 3, 4, 5})
    )));
    Iterable<Cell<?>> cells = (Iterable<Cell<?>>) ((UpdateOperation) intrinsic).apply(null);
    List<Cell<?>> applied = new ArrayList<>();
    cells.forEach(applied::add);
    assertThat(applied, is(Arrays.asList(
        Cell.cell("string", "en français, s'il vous plaît."),
        Cell.cell("int", 21),
        Cell.cell("long", 1000L),
        Cell.cell("boolean", true),
        Cell.cell("char", '%'),
        Cell.cell("double", Double.NaN),
        Cell.cell("bytes", new byte[]{1, 2, 3, 4, 5})
    )));
  }

  @Test
  public void testAndPredicate() {
    Predicate<Record<?>> and = Functions.extractString(defineString("name")).is("joe").and(Functions.extractString(defineString("address")).is("joe's street"));
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(and);
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList())), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("address", "joe's street")))), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("name", "joe")))), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("address", "joe's street"), Cell.cell("name", "joe")))), is(true));
  }

  @Test
  public void testOrPredicate() {
    Predicate<Record<?>> or = Functions.extractString(defineString("name")).is("joe").or(Functions.extractString(defineString("address")).is("joe's street"));
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(or);
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList())), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("address", "joe's street")))), is(true));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("name", "joe")))), is(true));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("address", "joe's street"), Cell.cell("name", "joe")))), is(true));
  }

  @Test
  public void testNegatePredicate() {
    Predicate<Record<?>> negate = Functions.extractString(defineString("name")).is("joe").negate();
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(negate);
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList())), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("address", "joe's street")))), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("name", "joe")))), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("name", "william")))), is(true));
  }

  @Test
  public void testCellValueEqualsPredicate() {
    Predicate<Record<?>> equals = Functions.extractString(defineString("name")).is("joe");
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(equals);
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("address", "some street")))), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("name", "joe")))), is(true));
  }

  @Test
  public void testCellValueGreaterThanPredicate() {
    Predicate<Record<?>> greaterThan = Functions.extractString(defineString("name")).isGreaterThan("joe");
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(greaterThan);
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("address", "some street")))), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("name", "joe")))), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("name", "william")))), is(true));
  }

  @Test
  public void testCellValueLessThanPredicate() {
    Predicate<Record<?>> lessThan = Functions.extractString(defineString("name")).isLessThan("joe");
    Predicate<Record<?>> predicate = encodeAndDecodePredicate(lessThan);
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("address", "some street")))), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("name", "joe")))), is(false));
    assertThat(predicate.test(new TestRecord<>(1, Arrays.asList(Cell.cell("name", "averell")))), is(true));
  }

  @Test
  public void testConstant() {
    Constant<Record<?>, String> constant = new Constant<>("joe");
    Function<Record<?>, String> intrinsic = encodeAndDecodeFunction(constant);
    assertThat(intrinsic.apply(null), is("joe"));
  }

  @Test
  public void testCellExtraction() {
    Function<Record<?>, Optional<String>> extractor = Functions.extractString(defineString("name"));
    Function<Record<?>, Optional<String>> intrinsic = encodeAndDecodeFunction(extractor);
    assertThat(intrinsic.apply(new TestRecord<>(1, Arrays.asList(Cell.cell("address", "some street")))), is(empty()));
    assertThat(intrinsic.apply(new TestRecord<>(1, Arrays.asList(Cell.cell("name", "joe")))), is(of("joe")));
  }

  @Test
  public void testBooleanCellPredicateExtraction() {
    Predicate<Record<?>> equals = CellDefinition.defineBool("equals").value().is(true);
    Predicate<Record<?>> intrinsic = encodeAndDecodePredicate(equals);
    assertThat(intrinsic.test(new TestRecord<>(1, Arrays.asList(Cell.cell("name", "joe")))), is(false));
    assertThat(intrinsic.test(new TestRecord<>(1, Arrays.asList(Cell.cell("equals", true)))), is(true));
  }

  @Test
  public void testKeyExtractor() throws Exception {
    Intrinsic intrinsic = encodeAndDecode(Record.keyFunction());
    Function<Record<String>, String> keyExtractor = (Function<Record<String>, String>)intrinsic;
    String key = keyExtractor.apply(new TestRecord<>("key", Arrays.asList(Cell.cell("address", "some street"))));
    assertThat(key, is("key"));
  }

  @Test
  public void testWaypointMarker() {
    WaypointMarker<String> marker = new WaypointMarker<>(8675309);
    Intrinsic intrinsic = encodeAndDecode(marker);
    assertThat(marker, equalTo(intrinsic));
    assertEquals(marker.hashCode(), intrinsic.hashCode());
    assertThat(intrinsic, is(instanceOf(WaypointMarker.class)));
    assertThat(intrinsic.incoming(), is(Matchers.empty()));
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.WAYPOINT));
    assertThat(((WaypointMarker)intrinsic).getWaypointId(), is(8675309));
  }

  @Test
  public void testIdentityFunction() {
    Function<?, ?> identityFunction = identityFunction();
    Intrinsic intrinsic = encodeAndDecode(identityFunction);
    assertThat(identityFunction, equalTo(intrinsic));
    assertEquals(identityFunction.hashCode(), intrinsic.hashCode());
    assertThat(intrinsic.getIntrinsicType(), is(IDENTITY_FUNCTION));
  }

  @Test
  public void testRecordKey() {
    RecordKey<String> recordKey = new RecordKey<>();
    Intrinsic intrinsic = encodeAndDecode(recordKey);
    assertThat(recordKey, equalTo(intrinsic));
    assertEquals(recordKey.hashCode(), intrinsic.hashCode());
    assertThat(intrinsic.getIntrinsicType(), is(FUNCTION_RECORD_KEY));
  }

  @Test
  public void testInputMapper() throws Exception {
    Intrinsic intrinsic = encodeAndDecode(Functions.inputMapper(identityFunction()));
    assertThat(intrinsic.getIntrinsicType(), is(INPUT_MAPPER));
    assertThat(intrinsic, is(instanceOf(InputMapper.class)));
    assertThat(((InputMapper)intrinsic).getMapper().getIntrinsicType(), is(IDENTITY_FUNCTION));
  }

  @Test
  public void testOutputMapper() throws Exception {
    Intrinsic intrinsic = encodeAndDecode(Functions.outputMapper(identityFunction()));
    assertThat(intrinsic.getIntrinsicType(), is(OUTPUT_MAPPER));
    assertThat(intrinsic, is(instanceOf(OutputMapper.class)));
    assertThat(((OutputMapper)intrinsic).getMapper().getIntrinsicType(), is(IDENTITY_FUNCTION));
  }

  @Test
  public void testPredicateFunctionAdapter() {
    PredicateFunctionAdapter<Object> adapter = new PredicateFunctionAdapter<>(AlwaysTrue.alwaysTrue());
    Intrinsic intrinsic = encodeAndDecode(adapter);
    assertThat(adapter, equalTo(intrinsic));
    assertEquals(adapter.hashCode(), intrinsic.hashCode());
    assertThat(intrinsic.getIntrinsicType(), is(FUNCTION_PREDICATE_ADAPTER));
    assertThat(intrinsic, instanceOf(PredicateFunctionAdapter.class));
    assertThat(((PredicateFunctionAdapter) intrinsic).getDelegate(), is(AlwaysTrue.alwaysTrue()));
  }

  @Test
  public void testToIntFunctionAdapter() {
    ToIntFunctionAdapter<Record<?>> adapter = new ToIntFunctionAdapter<>((IntrinsicToIntFunction<Record<?>>) defineInt("FOO").intValueOr(0));
    Intrinsic intrinsic = encodeAndDecode(adapter);
    assertThat(adapter, equalTo(intrinsic));
    assertEquals(adapter.hashCode(), intrinsic.hashCode());
    assertThat(intrinsic.getIntrinsicType(), is(FUNCTION_TO_INT_ADAPTER));
    assertThat(intrinsic, instanceOf(ToIntFunctionAdapter.class));
    assertThat(((ToIntFunctionAdapter<?>) intrinsic).getDelegate(), instanceOf(CellValue.IntCellValue.class));
  }

  @Test
  public void testToLongFunctionAdapter() {
    ToLongFunctionAdapter<Record<?>> adapter = new ToLongFunctionAdapter<>((IntrinsicToLongFunction<Record<?>>) defineLong("FOO").longValueOr(0L));
    Intrinsic intrinsic = encodeAndDecode(adapter);
    assertThat(adapter, equalTo(intrinsic));
    assertEquals(adapter.hashCode(), intrinsic.hashCode());
    assertThat(intrinsic.getIntrinsicType(), is(FUNCTION_TO_LONG_ADAPTER));
    assertThat(intrinsic, instanceOf(ToLongFunctionAdapter.class));
    assertThat(((ToLongFunctionAdapter<?>) intrinsic).getDelegate(), instanceOf(CellValue.LongCellValue.class));
  }

  @Test
  public void testToDoubleFunctionAdapter() {
    ToDoubleFunctionAdapter<Record<?>> adapter = new ToDoubleFunctionAdapter<>((IntrinsicToDoubleFunction<Record<?>>) defineDouble("FOO").doubleValueOr(0.0));
    Intrinsic intrinsic = encodeAndDecode(adapter);
    assertThat(adapter, equalTo(intrinsic));
    assertEquals(adapter.hashCode(), intrinsic.hashCode());
    assertThat(intrinsic.getIntrinsicType(), is(FUNCTION_TO_DOUBLE_ADAPTER));
    assertThat(intrinsic, instanceOf(ToDoubleFunctionAdapter.class));
    assertThat(((ToDoubleFunctionAdapter<?>) intrinsic).getDelegate(), instanceOf(CellValue.DoubleCellValue.class));
  }

  @Test
  public void testIntrinsicLogger() {
    List<IntrinsicFunction<? super Double, ?>> mappers = Arrays.asList(new Constant<>(0.0));
    Intrinsic logger = new IntrinsicLogger<>("a", mappers);
    IntrinsicLogger<?> decoded = (IntrinsicLogger) encodeAndDecode(logger);
    assertThat(logger, equalTo(decoded));
    assertEquals(logger.hashCode(), decoded.hashCode());
    assertThat(decoded.getIntrinsicType(), is(CONSUMER_LOG));
    assertThat(decoded.getMessage(), is("a"));
    assertThat(decoded.getMappers(), is(mappers));
  }

  @Test
  public void testComparableComparator() {
    Intrinsic logger = new ComparableComparator<>(new Constant<>(0.0));
    ComparableComparator<?, ?> decoded = (ComparableComparator) encodeAndDecode(logger);
    assertThat(logger, equalTo(decoded));
    assertEquals(logger.hashCode(), decoded.hashCode());
    assertThat(decoded.getIntrinsicType(), is(COMPARATOR));
    assertThat(decoded.getFunction(), is(new Constant<>(0.0)));
  }

  @Test
  public void testReverseComparableComparator() {
    final ComparableComparator<Object, Double> comparator = new ComparableComparator<>(new Constant<>(0.0));
    Intrinsic logger = new ReverseComparableComparator<>(comparator);
    ReverseComparableComparator<?, ?> decoded = (ReverseComparableComparator) encodeAndDecode(logger);
    assertThat(logger, equalTo(decoded));
    assertEquals(logger.hashCode(), decoded.hashCode());
    assertThat(decoded.getIntrinsicType(), is(REVERSE_COMPARATOR));
    assertThat(decoded.getOriginalComparator(), is(comparator));
  }

  @Test
  public void testTupleFirst() throws Exception {
    Intrinsic intrinsic = encodeAndDecode(Functions.tupleFirst());
    assertThat(intrinsic.getIntrinsicType(), is(TUPLE_FIRST));
    assertThat(intrinsic, is(instanceOf(BuildableFunction.class)));
    assertThat(((BuildableFunction)intrinsic).apply(Tuple.of(1, 2)), is(1));
  }

  @Test
  public void testTupleSecond() throws Exception {
    Intrinsic intrinsic = encodeAndDecode(Functions.tupleSecond());
    assertThat(intrinsic.getIntrinsicType(), is(TUPLE_SECOND));
    assertThat(intrinsic, is(instanceOf(BuildableFunction.class)));
    assertThat(((BuildableFunction)intrinsic).apply(Tuple.of(1, 2)), is(2));
  }

  @Test
  public void testCountingCollector() throws Exception {
    Intrinsic intrinsic = encodeAndDecode(Functions.countingCollector());
    assertThat(intrinsic.getIntrinsicType(), is(COUNTING_COLLECTOR));
    assertThat(intrinsic, is(instanceOf(Collector.class)));
    Collector<String, Object, ?> collector = (Collector)intrinsic;
    Object container = ((Collector)intrinsic).supplier().get();
    collector.accumulator().accept(container, "foo");
    assertThat(collector.finisher().apply(container), is(1L));
  }

  @Test
  public void testDefaultGroupingCollector() {
    Intrinsic intrinsic = encodeAndDecode(Functions.groupingByCollector(
            defineString("address").valueOrFail(),
            Functions.countingCollector()
    ));
    assertThat(intrinsic.getIntrinsicType(), is(COLLECTOR_GROUPING));
    assertThat(intrinsic, instanceOf(DefaultGroupingCollector.class));
    Collector<Record<?>, ?, Map<String, Long>> collector = (Collector) intrinsic;

    Object supplied = collector.supplier().get();
    assertThat(supplied, instanceOf(Map.class));
    assertThat(supplied, not(instanceOf(ConcurrentMap.class)));
    Map<String, Long> container = (Map) supplied;
    BiConsumer<Map<String, Long>, Record<Long>> accumulator = (BiConsumer) collector.accumulator();
    accumulator.accept(container,
            new TestRecord<>(1L, Arrays.asList(Cell.cell("address", "some street"))));
    Function<Map<String, Long>, Map<String, Long>> finisher = (Function<Map<String, Long>, Map<String, Long>>)
            collector.finisher();
    Map<String, Long> result = finisher
            .apply(container);
    assertThat(result.keySet(), containsInAnyOrder("some street"));
    assertThat(result.values(), containsInAnyOrder(1L));
  }

  @Test
  public void testConcurrentGroupingCollector() {
    Intrinsic intrinsic = encodeAndDecode(Functions.groupingByConcurrentCollector(
            defineString("address").valueOrFail(),
            Functions.countingCollector()
    ));
    assertThat(intrinsic.getIntrinsicType(), is(COLLECTOR_GROUPING_CONCURRENT));
    assertThat(intrinsic, instanceOf(ConcurrentGroupingCollector.class));
    Collector<Record<?>, ?, Map<String, Long>> collector = (Collector) intrinsic;

    Object supplied = collector.supplier().get();
    assertThat(supplied, instanceOf(ConcurrentMap.class));
    Map<String, Long> container = (Map) supplied;
    BiConsumer<Map<String, Long>, Record<Long>> accumulator = (BiConsumer) collector.accumulator();
    accumulator.accept(container,
            new TestRecord<>(1L, Arrays.asList(Cell.cell("address", "some street"))));
    Function<Map<String, Long>, Map<String, Long>> finisher = (Function<Map<String, Long>, Map<String, Long>>)
            collector.finisher();
    Map<String, Long> result = finisher
            .apply(container);
    assertThat(result.keySet(), containsInAnyOrder("some street"));
    assertThat(result.values(), containsInAnyOrder(1L));
  }

  @Test
  public void testPartitioningCollector() {
    Intrinsic intrinsic = encodeAndDecode(Functions.partitioningCollector(
            defineBool("active").isTrue(),
            Functions.countingCollector()
    ));
    assertThat(intrinsic.getIntrinsicType(), is(COLLECTOR_PARTITIONING));
    assertThat(intrinsic, instanceOf(PartitioningCollector.class));
    Collector<Record<?>, ?, Map<Boolean, Long>> collector = (Collector) intrinsic;

    Map<Integer, Long> container = (Map) collector.supplier().get();

    BiConsumer<Map<Integer, Long>, Record<Long>> biConsumer = (BiConsumer) collector.accumulator();
    biConsumer.accept(container, new TestRecord<>(1L, singletonList(cell("active", true))));

    Function<Map<Integer, Long>, Map<Boolean, Long>> function = (Function<Map<Integer, Long>, Map<Boolean, Long>>)
            collector.finisher();
    Map<Boolean, Long> result = function.apply(container);
    assertThat(result.keySet(), containsInAnyOrder(true, false));
    assertThat(result.get(true), Matchers.is(1L));
    assertThat(result.get(false), Matchers.is(0L));
  }

  @Test
  public void testMappingCollector() {
    Intrinsic intrinsic = encodeAndDecode(Functions.mappingCollector(
            defineInt("age").intValueOrFail().increment().boxed(),
            Functions.countingCollector()
    ));
    assertThat(intrinsic.getIntrinsicType(), is(COLLECTOR_MAPPING));
    assertThat(intrinsic, instanceOf(MappingCollector.class));
    Collector<Record<Integer>, ?, Long> collector = (Collector) intrinsic;
    Object container = collector.supplier().get();

    BiConsumer<Object, Record<Integer>> biConsumer = (BiConsumer<Object, Record<Integer>>)
            collector.accumulator();
    biConsumer.accept(container,
            new TestRecord<>(1, singletonList(cell("age", 10))));

    Function<Object, Long> finisher = (Function<Object, Long>) collector.finisher();
    Long result = finisher.apply(container);
    assertThat(result, Matchers.is(1L));
  }

  @Test
  public void testFilteringCollector() {
    Intrinsic intrinsic = encodeAndDecode(Functions.filteringCollector(
            defineInt("age").valueOrFail().is(10),
            Functions.countingCollector()
    ));
    assertThat(intrinsic.getIntrinsicType(), is(COLLECTOR_FILTERING));
    assertThat(intrinsic, instanceOf(FilteringCollector.class));
    Collector<Record<?>, ?, Long> collector = (Collector) intrinsic;
    Object container = collector.supplier().get();

    BiConsumer<Object, Record<?>> biConsumer = (BiConsumer<Object, Record<?>>) collector.accumulator();
    biConsumer.accept(container,
            new TestRecord<>(1, singletonList(cell("age", 10))));

    Function<Object, Long> finisher = (Function<Object, Long>) collector.finisher();
    Long result = finisher.apply(container);
    assertThat(result, Matchers.is(1L));
  }

  @Test
  public void testNonPortableTransform() throws Exception {
    Intrinsic intrinsic = encodeAndDecode(new NonPortableTransform<String>(8675309));
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.NON_PORTABLE_TRANSFORM));
    assertThat(intrinsic.incoming(), is(Matchers.empty()));
    assertThat(intrinsic, is(instanceOf(NonPortableTransform.class)));
    assertThat(((NonPortableTransform)intrinsic).getWaypointId(), is(8675309));
  }

  @Test
  public void testNonGatedEquals() {
    Constant<Object, Long> left = new Constant<>(0L);
    Constant<Object, Long> right = new Constant<>(1L);
    NonGatedComparison.Equals<Object, Long> equals = new NonGatedComparison.Equals<>(left, right);
    Intrinsic intrinsic = encodeAndDecode(equals);
    assertThat(equals, equalTo(intrinsic));
    assertEquals(equals.hashCode(), intrinsic.hashCode());
    assertThat(intrinsic.getIntrinsicType(), is(PREDICATE_EQUALS));
    assertThat(intrinsic.incoming().stream().map(i -> ((Constant)i).getValue()).collect(toList()), contains(right.getValue(), left.getValue()));
    assertThat(intrinsic, is(instanceOf(NonGatedComparison.Equals.class)));
  }

  @Test
  public void testNonGatedContrast() {
    Constant<Object, Long> left = new Constant<>(0L);
    Constant<Object, Long> right = new Constant<>(1L);
    EnumSet<ComparisonType> comparisons = EnumSet.complementOf(EnumSet.of(ComparisonType.EQ, ComparisonType.NEQ));
    for (ComparisonType comparison : comparisons) {
      NonGatedComparison.Contrast<Object, Long> contrast = new NonGatedComparison.Contrast<>(left, comparison, right);
      Intrinsic intrinsic = encodeAndDecode(contrast);
      assertThat(contrast, equalTo(intrinsic));
      assertEquals(contrast.hashCode(), intrinsic.hashCode());
      assertThat(intrinsic.getIntrinsicType(), is(PREDICATE_CONTRAST));
      assertThat(intrinsic.incoming().stream().map(i -> ((Constant)i).getValue()).collect(toList()), contains(right.getValue(), left.getValue()));
      assertThat(intrinsic, is(instanceOf(NonGatedComparison.Contrast.class)));
      assertThat(((NonGatedComparison.Contrast)intrinsic).getComparisonType(), is(comparison));
    }
  }

  private Cell<Double> doubleCell(String name, Double value) {
    return defineDouble(name).newCell(value);
  }

  private Cell<Long> longCell(String name, Long value) {
    return defineLong(name).newCell(value);
  }

  private Cell<Integer> intCell(String name, Integer value) {
    return defineInt(name).newCell(value);
  }

  private Cell<String> stringCell(String name, String value) {
    return defineString(name).newCell(value);
  }

  private void assertDoubleResult(ToDoubleFunction<Record<?>> function, double expected, Collection<Cell<?>> containedCells) {
    ToDoubleFunction<Record<?>> intrinsic = (ToDoubleFunction<Record<?>>) encodeAndDecode(function);
    assertThat(function, equalTo(intrinsic));
    assertEquals(function.hashCode(), intrinsic.hashCode());
    double result = intrinsic.applyAsDouble(new TestRecord<>("aKey", containedCells));
    assertThat(result, is(expected));
  }

  private void assertLongResult(ToLongFunction<Record<?>> function, long expected, Collection<Cell<?>> containedCells) {
    ToLongFunction<Record<?>> intrinsic = (ToLongFunction<Record<?>>) encodeAndDecode(function);
    assertThat(function, equalTo(intrinsic));
    assertEquals(function.hashCode(), intrinsic.hashCode());
    long result = intrinsic.applyAsLong(new TestRecord<>("aKey", containedCells));
    assertThat(result, is(expected));
  }

  private void assertIntResult(ToIntFunction<Record<?>> function, int expected, Collection<Cell<?>> containedCells) {
    ToIntFunction<Record<?>> intrinsic = (ToIntFunction<Record<?>>) encodeAndDecode(function);
    assertThat(function, equalTo(intrinsic));
    assertEquals(function.hashCode(), intrinsic.hashCode());
    int result = intrinsic.applyAsInt(new TestRecord<>("aKey", containedCells));
    assertThat(result, is(expected));
  }

  private void assertContains(UpdateOperation<String> updateOperation, Cell<?> expectedCell) {
    assertContains(updateOperation, Arrays.asList(expectedCell), Collections.emptyList());
  }

  private void assertContains(UpdateOperation<String> updateOperation, Collection<Cell<?>> expectedCells, Collection<Cell<?>> containedCells) {
    UpdateOperation<String> intrinsic = (UpdateOperation<String>) encodeAndDecode(updateOperation);
    Iterable<Cell<?>> aRecord = intrinsic.apply(new TestRecord<>("aKey", containedCells));
    assertThat(aRecord, containsInAnyOrder(expectedCells.toArray()));
  }

  private void assertComparison(Comparator<Record<?>> comparator, Cell<?> cell1, Cell<?> cell2, int expect) {
    Comparator<Record<?>> intrinsic = (Comparator<Record<?>>) encodeAndDecode(comparator);
    int rc = intrinsic.compare(new TestRecord<>("key1", Arrays.asList(cell1)), new TestRecord<>("key2", Arrays.asList(cell2)));
    assertThat(rc, is(expect));
  }

}
