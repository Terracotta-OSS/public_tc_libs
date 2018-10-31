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
package com.terracottatech.store.common.messages.stream;

import com.terracottatech.store.Record;
import com.terracottatech.store.TestRecord;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.common.messages.RecordData;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.intrinsics.IdentityFunction;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Test;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.definition.CellDefinition.defineBool;
import static com.terracottatech.store.definition.CellDefinition.defineDouble;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.function.Collectors.counting;
import static com.terracottatech.store.function.Collectors.filtering;
import static com.terracottatech.store.function.Collectors.groupingBy;
import static com.terracottatech.store.function.Collectors.groupingByConcurrent;
import static com.terracottatech.store.function.Collectors.mapping;
import static com.terracottatech.store.function.Collectors.partitioningBy;
import static com.terracottatech.store.function.Collectors.summarizingDouble;
import static com.terracottatech.store.function.Collectors.summarizingInt;
import static com.terracottatech.store.function.Collectors.summarizingLong;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.function.Function.identity;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;


/**
 * @author Ludovic Orban
 */
public class ElementValueTest {

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupported() throws Exception {
    elementValue(singletonList(1));
  }

  @Test
  public void testNull() throws Exception {
    assertThat(encodeAndDecode(elementValue(null)).getValue(), is(nullValue()));
  }

  @Test
  public void testPrimitive() throws Exception {
    assertThat(encodeAndDecode(elementValue(1)).getValue(), is(1));
    assertThat(encodeAndDecode(elementValue(1L)).getValue(), is(1L));
    assertThat(encodeAndDecode(elementValue("joe")).getValue(), is("joe"));
    assertThat(encodeAndDecode(elementValue(Double.NaN)).getValue(), is(Double.NaN));
    assertThat(encodeAndDecode(elementValue(true)).getValue(), is(true));
    assertThat(encodeAndDecode(elementValue('Y')).getValue(), is('Y'));
    assertThat(encodeAndDecode(elementValue(new byte[] {1,2,3})).getValue(), is(new byte[] {1,2,3}));
  }

  @Test
  public void testCell() throws Exception {
    assertThat(encodeAndDecode(elementValue(cell("cell1", 1))).getValue(), is(cell("cell1", 1)));
    assertThat(encodeAndDecode(elementValue(cell("cell2", 1L))).getValue(), is(cell("cell2", 1L)));
    assertThat(encodeAndDecode(elementValue(cell("cell3", "joe"))).getValue(), is(cell("cell3", "joe")));
    assertThat(encodeAndDecode(elementValue(cell("cell4", 0.1))).getValue(), is(cell("cell4", 0.1)));
    assertThat(encodeAndDecode(elementValue(cell("cell5", false))).getValue(), is(cell("cell5", false)));
    assertThat(encodeAndDecode(elementValue(cell("cell6", 'X'))).getValue(), is(cell("cell6", 'X')));
    assertThat(encodeAndDecode(elementValue(cell("cell7", new byte[] {1,2,3}))).getValue(), is(cell("cell7", new byte[] {1,2,3})));
  }

  @Test
  public void testOptionalLong() throws Exception {
    assertThat(encodeAndDecode(elementValue(OptionalLong.of(Long.MAX_VALUE))).getValue(), is(OptionalLong.of(Long.MAX_VALUE)));
    assertThat(encodeAndDecode(elementValue(OptionalLong.of(Long.MIN_VALUE))).getValue(), is(OptionalLong.of(Long.MIN_VALUE)));
    assertThat(encodeAndDecode(elementValue(OptionalLong.of(0L))).getValue(), is(OptionalLong.of(0L)));
    assertThat(encodeAndDecode(elementValue(OptionalLong.empty())).getValue(), is(OptionalLong.empty()));
  }

  @Test
  public void testOptionalInt() throws Exception {
    assertThat(encodeAndDecode(elementValue(OptionalInt.of(Integer.MAX_VALUE))).getValue(), is(OptionalInt.of(Integer.MAX_VALUE)));
    assertThat(encodeAndDecode(elementValue(OptionalInt.of(Integer.MIN_VALUE))).getValue(), is(OptionalInt.of(Integer.MIN_VALUE)));
    assertThat(encodeAndDecode(elementValue(OptionalInt.of(0))).getValue(), is(OptionalInt.of(0)));
    assertThat(encodeAndDecode(elementValue(OptionalInt.empty())).getValue(), is(OptionalInt.empty()));
  }

  @Test
  public void testOptionalDouble() throws Exception {
    assertThat(encodeAndDecode(elementValue(OptionalDouble.of(Double.MAX_VALUE))).getValue(), is(OptionalDouble.of(Double.MAX_VALUE)));
    assertThat(encodeAndDecode(elementValue(OptionalDouble.of(Double.MIN_VALUE))).getValue(), is(OptionalDouble.of(Double.MIN_VALUE)));
    assertThat(encodeAndDecode(elementValue(OptionalDouble.of(Double.NaN))).getValue(), is(OptionalDouble.of(Double.NaN)));
    assertThat(encodeAndDecode(elementValue(OptionalDouble.empty())).getValue(), is(OptionalDouble.empty()));
  }

  @Test
  public void testIntSummaryStatistics() throws Exception {
    IntSummaryStatistics stats = new IntSummaryStatistics();
    stats.accept(0);
    stats.accept(5);
    stats.accept(10);
    stats.accept(15);
    stats.accept(20);
    stats.accept(25);

    IntSummaryStatistics copy = encodeAndDecode(elementValue(stats)).getValue();
    assertThat(copy.getCount(), is(stats.getCount()));
    assertThat(copy.getSum(), is(stats.getSum()));
    assertThat(copy.getMin(), is(stats.getMin()));
    assertThat(copy.getMax(), is(stats.getMax()));
  }

  @Test
  public void testLongSummaryStatistics() throws Exception {
    LongSummaryStatistics stats = new LongSummaryStatistics();
    stats.accept(0L);
    stats.accept(500L);
    stats.accept(1000L);
    stats.accept(1500L);
    stats.accept(2000L);
    stats.accept(2500L);

    LongSummaryStatistics copy = encodeAndDecode(elementValue(stats)).getValue();
    assertThat(copy.getCount(), is(stats.getCount()));
    assertThat(copy.getSum(), is(stats.getSum()));
    assertThat(copy.getMin(), is(stats.getMin()));
    assertThat(copy.getMax(), is(stats.getMax()));
  }

  @Test
  public void testDoubleSummaryStatistics() throws Exception {
    DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
    stats.accept(.0);
    stats.accept(.05);
    stats.accept(.10);
    stats.accept(.15);
    stats.accept(.20);
    stats.accept(.25);

    DoubleSummaryStatistics copy = encodeAndDecode(elementValue(stats)).getValue();
    assertThat(copy.getCount(), is(stats.getCount()));
    assertThat(copy.getSum(), is(stats.getSum()));
    assertThat(copy.getMin(), is(stats.getMin()));
    assertThat(copy.getMax(), is(stats.getMax()));
  }

  @Test
  public void testOptional() throws Exception {
    assertThat(encodeAndDecode(elementValue(Optional.empty())).getValue(this::toRecord), is(Optional.empty()));
    assertThat(encodeAndDecode(elementValue(Optional.of(1))).getValue(this::toRecord), is(Optional.of(1)));
    assertThat(encodeAndDecode(elementValue(Optional.of(Optional.empty()))).getValue(this::toRecord), is(Optional.of(Optional.empty())));
    assertThat(encodeAndDecode(elementValue(Optional.of(Optional.of(OptionalLong.empty())))).getValue(this::toRecord), is(Optional.of(Optional.of(OptionalLong.empty()))));
    assertThat(encodeAndDecode(elementValue(Optional.of(Optional.of(OptionalLong.of(100L))))).getValue(this::toRecord), is(Optional.of(Optional.of(OptionalLong.of(100L)))));

    Optional<Optional<TestRecord<String>>> optOptRec = encodeAndDecode(elementValue(Optional.of(Optional.of(new TestRecord<>(123L, "aKey", asList(cell("cell1", "value1"), cell("cell2", 2))))))).getValue(this::toRecord);
    TestRecord<String> dataValue = optOptRec.get().get();

    assertThat(dataValue.getMSN(), is(123L));
    assertThat(dataValue.getKey(), is("aKey"));
    assertThat(dataValue, IsIterableContainingInOrder.contains(cell("cell1", "value1"), cell("cell2", 2)));
  }

  @Test
  public void testRecordData() throws Exception {
    ElementValue decoded = encodeAndDecode(elementValue(new TestRecord<>(123L, "aKey", asList(cell("cell1", "value1"), cell("cell2", 2)))));

    TestRecord<String> dataValue = decoded.getValue(this::toRecord);
    assertThat(dataValue.getMSN(), is(123L));
    assertThat(dataValue.getKey(), is("aKey"));
    assertThat(dataValue, IsIterableContainingInAnyOrder.containsInAnyOrder(cell("cell1", "value1"), cell("cell2", 2)));
  }

  @Test
  public void testRecordDataTuple() throws Exception {
    TestRecord<String> firstRecord = new TestRecord<>(1L, "first",
        asList(cell("long", 8675309L), cell("boolean", false)));
    TestRecord<String> secondRecord = new TestRecord<>(2L, "second",
        asList(cell("long", 8467110L), cell("boolean", true)));
    Tuple<RecordData<String>, RecordData<String>> tuple = Tuple.of(toData(firstRecord), toData(secondRecord));

    ElementValue decoded = encodeAndDecode(elementValue(tuple));
    @SuppressWarnings("unchecked") Tuple<RecordData<String>, RecordData<String>> dataValue = decoded.getValue(this::toRecord);
    assertThat(toRecord(dataValue.getFirst()), is(firstRecord));
    assertThat(toRecord(dataValue.getSecond()), is(secondRecord));

    try {
      encodeAndDecode(elementValue(Tuple.of(null, toData(secondRecord))));
    } catch (UnsupportedOperationException e) {
      assertThat(e.getMessage(), allOf(containsString("Unsupported element value"), containsString(Tuple.class.getName())));
    }
  }

  @Test
  public void testMapOfPrimitives() {
    checkDefaultMap(emptyMap());
    checkDefaultMap(singletonMap(null, 1));
    checkDefaultMap(singletonMap(1, null));
    checkDefaultMap(singletonMap(1, ""));

    Map<Integer, Long> groups = Stream.of("x", "zz", "y")
            .collect(groupingBy(String::length, counting()));
    checkDefaultMap(groups);

    Map<Boolean, Long> partitions = Stream.of('a', 'A', 'b')
            .collect(partitioningBy(Character::isUpperCase, counting()));
    checkDefaultMap(partitions);

    Map<String, Long> mapped = Stream.of("aa", "Aa", "b")
            .collect(mapping(String::toUpperCase, groupingBy(identity(), counting())));
    checkDefaultMap(mapped);

    Map<Character, Long> filtered = Stream.of('a', 'A', 'b', 'b')
            .collect(filtering(Character::isLowerCase, groupingBy(identity(), counting())));
    checkDefaultMap(filtered);
  }

  @Test
  public void testMapOfOptionalKeys() {
    checkDefaultMap(singletonMap(Optional.empty(), null));
    checkDefaultMap(singletonMap(Optional.of("key"), null));

    StringCellDefinition def = defineString("str");
    Record<Integer> record = new TestRecord<>(1,
            singletonList(cell("str", "xxx")));
    Map<Optional<Integer>, Long> optLenGroups = Stream.of(record)
            .collect(groupingBy(def.value().length(), counting()));
    checkDefaultMap(optLenGroups);
  }

  @Test
  public void testMapOfRecordKeys() {
    Record<Integer> record = new TestRecord<>(1,
            singletonList(cell("str", "xxx")));
    checkDefaultMap(singletonMap(record, null));

    Map<Record<Integer>, Long> recordCounts = Stream.of(record)
            .collect(groupingBy(new IdentityFunction<>(), counting()));
    checkDefaultMap(recordCounts);
  }

  @Test
  public void testMapOfSummaryStatistics() {
    checkMapStrings(singletonMap(1, new IntSummaryStatistics()));
    checkMapStrings(singletonMap(1, new LongSummaryStatistics()));
    checkMapStrings(singletonMap(1, new DoubleSummaryStatistics()));

    IntCellDefinition intCell = defineInt("int");
    LongCellDefinition longCell = defineLong("long");
    DoubleCellDefinition doubcleCell = defineDouble("double");
    BoolCellDefinition boolCell = defineBool("bool");

    Record<Integer> record = new TestRecord<>(1,
            asList(
                    cell("int", 1),
                    cell("long", 1L),
                    cell("double", 1.0),
                    cell("bool", true)
            ));

    Map<Object, IntSummaryStatistics> intValueSummaries = Stream.of(record)
            .collect(groupingBy(Record::getKey,
                    summarizingInt(intCell.intValueOrFail())));
    checkMapStrings(intValueSummaries);

    Map<Boolean, LongSummaryStatistics> longValueSummaries = Stream.of(record)
            .collect(partitioningBy(boolCell.isTrue(),
                    summarizingLong(longCell.longValueOrFail())));
    checkMapStrings(longValueSummaries);

    Map<Optional<Integer>, DoubleSummaryStatistics> doubleValueSummaries = Stream.of(record)
            .collect(groupingBy(intCell.value(),
                    summarizingDouble(doubcleCell.doubleValueOrFail())));
    checkMapStrings(doubleValueSummaries);
  }

  /**
   * Use String representations since XXXSummaryStatistics classes don't
   * override equals and hashCode.
   */
  private void checkMapStrings(Map<?, ?> map) {
    Map<?, ?> encoded = encodeAndDecode(elementValue(map))
            .getValue(this::toRecord);
    assertThat(encoded.toString(), is(map.toString()));
  }

  private void checkDefaultMap(Map<?, ?> map) {
    assertThat(encodeAndDecodeMap(map), not(instanceOf(ConcurrentMap.class)));
  }

  private Map<?, ?> encodeAndDecodeMap(Map<?, ?> map) {
    Map<?, ?> encoded = encodeAndDecode(elementValue(map))
            .getValue(this::toRecord);
    assertThat(encoded, is(map));
    return encoded;
  }

  @Test
  public void testConcurrentMap() {
    checkConcurrentMap(new ConcurrentHashMap<>());

    ConcurrentMap<?, ?> groups = Stream.of("x", "zz", "y")
            .collect(groupingByConcurrent(String::length, counting()));
    checkConcurrentMap(groups);
  }

  private void checkConcurrentMap(ConcurrentMap<?, ?> map) {
    assertThat(encodeAndDecodeMap(map), instanceOf(ConcurrentMap.class));
  }

  private ElementValue elementValue(Object o) {
    return ElementValue.ValueType.forObject(o).createElementValue(o);
  }

  private Record<?> toRecord(RecordData<?> data) {
    return new TestRecord<>(data);
  }

  private <K extends Comparable<K>> RecordData<K> toData(Record<K> record) {
    TestRecord<K> tr = (TestRecord<K>) record;
    return new RecordData<>(tr.getMSN(), tr.getKey(), tr);
  }

  private static ElementValue encodeAndDecode(ElementValue elementValue) {
    StructEncoder<?> encoder = StreamStructures.ELEMENT_VALUE_STRUCT.encoder();
    StreamStructures.encodeElementValue(encoder, elementValue);
    ByteBuffer byteBuffer = encoder.encode();
    byteBuffer.rewind();
    StructDecoder<?> decoder = StreamStructures.ELEMENT_VALUE_STRUCT.decoder(byteBuffer);
    return StreamStructures.decodeElementValue(decoder);
  }

}
