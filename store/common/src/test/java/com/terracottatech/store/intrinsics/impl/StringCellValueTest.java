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
package com.terracottatech.store.intrinsics.impl;

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.function.BuildableToIntFunction;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicType;
import com.terracottatech.store.intrinsics.impl.CellValue.StringCellValue;
import org.junit.Test;

import java.util.Comparator;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Basic tests for {@link StringCellValue}.
 */
public class StringCellValueTest extends ComparableCellValueTest<String> {

  private final StringCellDefinition stringCell = CellDefinition.defineString("stringCell");

  @Override
  protected StringCellDefinition getCellDefinition() {
    return stringCell;
  }

  @Override
  protected final String getCellValue() {
    return "value";
  }

  @Override
  protected String getGreaterValue() {
    return "zebra";
  }

  @Override
  protected StringCellValue getComparableCellValue(String defaultValue) {
    return new StringCellValue(stringCell, defaultValue);
  }

  @Test
  public void testObjectMethods() {
    StringCellValue cellValue = getComparableCellValue("value");
    assertEquality(cellValue, cellValue);

    StringCellValue same = getComparableCellValue("value");
    assertEquality(cellValue, same);

    StringCellValue otherValue = getComparableCellValue("other");
    assertNotEquals(cellValue, otherValue);
  }

  @Test
  public void testLength() {
    Record<?> valueRecord = getValueRecord("value");

    StringCellValue cellValue = getComparableCellValue("value");

    BuildableToIntFunction<Record<?>> length = cellValue.length();
    assertThat(length, is(instanceOf(Intrinsic.class)));
    Intrinsic intrinsic = (Intrinsic) length;
    IntrinsicType intrinsicType = intrinsic.getIntrinsicType();
    assertNotNull(intrinsicType);
    assertEquals(IntrinsicType.STRING_LENGTH, intrinsicType);

    Predicate<Record<?>> predicate = length.is(5);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertTrue(predicate.test(valueRecord));
    assertFalse(predicate.negate().test(valueRecord));

    Comparator<Record<?>> comparator = length.asComparator();
    assertThat(comparator, is(instanceOf(Intrinsic.class)));

    Record<?> equalRecord = getValueRecord("value");
    assertEquals(0, comparator.compare(valueRecord, equalRecord));

    Record<?> longerRecord = getValueRecord("longerValue");
    assertEquals(-1, comparator.compare(valueRecord, longerRecord));
    assertEquals(1, comparator.compare(longerRecord, valueRecord));

    assertEquality(length, length);

    StringCellValue same = getComparableCellValue("value");
    BuildableToIntFunction<Record<?>> sameLength = same.length();
    assertEquality(length, sameLength);

    StringCellValue other = getComparableCellValue("other");
    BuildableToIntFunction<Record<?>> otherLength = other.length();
    assertInequality(length, otherLength);
  }

  @Test
  public void testStartsWith() {
    StringCellValue cellValue = getComparableCellValue("defaultValue");

    BuildablePredicate<Record<?>> startsWith = cellValue.startsWith("prefix");
    assertThat(startsWith, is(instanceOf(Intrinsic.class)));
    Intrinsic intrinsic = (Intrinsic) startsWith;
    IntrinsicType intrinsicType = intrinsic.getIntrinsicType();
    assertNotNull(intrinsicType);
    assertEquals(IntrinsicType.STRING_STARTS_WITH, intrinsicType);

    assertTrue(startsWith.test(getValueRecord("prefixedValue")));
    assertFalse(startsWith.test(getValueRecord("value")));
    assertFalse(startsWith.test(new TestRecord<>("valueRecord", emptyList())));

    assertEquality(startsWith, startsWith);

    StringCellValue same = getComparableCellValue("defaultValue");
    BuildablePredicate<Record<?>> samePrefix = same.startsWith("prefix");
    assertEquality(startsWith, samePrefix);
    BuildablePredicate<Record<?>> otherPrefix = same.startsWith("otherPrefix");
    assertInequality(startsWith, otherPrefix);

    StringCellValue other = getComparableCellValue("other");
    BuildablePredicate<Record<?>> otherStartsWith = other.startsWith("prefix");
    assertInequality(startsWith, otherStartsWith);
  }

  private void assertEquality(Object left, Object right) {
    assertEquals(left, right);
    assertEquals(left.toString(), right.toString());
    assertEquals(left.hashCode(), right.hashCode());
  }

  private void assertInequality(Object left, Object right) {
    assertNotEquals(left, right);
    assertNotEquals(left.toString(), right.toString());
  }
}