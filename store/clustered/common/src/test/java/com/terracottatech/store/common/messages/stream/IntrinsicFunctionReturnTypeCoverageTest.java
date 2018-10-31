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
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.BytesCellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import org.hamcrest.Matcher;
import org.junit.Test;

import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.common.messages.stream.ElementValue.ValueType;
import static com.terracottatech.store.definition.CellDefinition.defineBool;
import static com.terracottatech.store.definition.CellDefinition.defineBytes;
import static com.terracottatech.store.definition.CellDefinition.defineChar;
import static com.terracottatech.store.definition.CellDefinition.defineDouble;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;

/**
 * Test that all return values of IntrinsicFunctions can be converted to ElementValues.
 */
public class IntrinsicFunctionReturnTypeCoverageTest
        extends ReturnTypeCoverageTest<IntrinsicFunction<Object, ?>> {

  private static final TestRecord<?> UNIVERSAL_RECORD = new TestRecord<>("foo", asList(
          cell("booleanCell", true),
          cell("charCell", "c"),
          cell("intCell", 0),
          cell("longCell", 0L),
          cell("stringCell", ""),
          cell("doubleCell", 0.0),
          cell("byteCell", new byte[]{})
  ));

  public IntrinsicFunctionReturnTypeCoverageTest() {
    super(IntrinsicFunction.class, asList(
            "foo",
            42,
            defineInt("bar"),
            defineInt("bar").value().isGreaterThan(0),
            defineInt("bar").intValueOr(0),
            defineInt("bar").valueOr(0).asComparator(),
            defineLong("bar").longValueOr(0),
            defineDouble("bar").doubleValueOr(0)
    ));
  }

  @Test
  public void testBooleanCellDefinition() {
    testCompleteness(defineBool("booleanCell"), BoolCellDefinition.class);
  }

  @Test
  public void testCharCellDefinition() {
    testCompleteness(defineChar("charCell"), CharCellDefinition.class);
  }

  @Test
  public void testIntegerCellDefinition() {
    testCompleteness(defineInt("intCell"), IntCellDefinition.class);
  }

  @Test
  public void testLongCellDefinition() {
    testCompleteness(defineLong("longCell"), LongCellDefinition.class);
  }

  @Test
  public void testDoubleCellDefinition() {
    testCompleteness(defineDouble("doubleCell"), DoubleCellDefinition.class);
  }

  @Test
  public void testStringCellDefinition() {
    testCompleteness(defineString("stringCell"), StringCellDefinition.class);
  }

  @Test
  public void testBytesCellDefinition() {
    testCompleteness(defineBytes("byteCell"), BytesCellDefinition.class);
  }

  @Test
  public void testRecordStatics() {
    testCompleteness(null, Record.class);
  }

  @Override
  Object apply(IntrinsicFunction<Object, ?> function) {
    return function.apply(UNIVERSAL_RECORD);
  }

  @Override
  Matcher<ValueType> typeMatcher() {
    return anyOf(
            is(ValueType.PRIMITIVE),
            is(ValueType.OPTIONAL)
    );
  }
}
