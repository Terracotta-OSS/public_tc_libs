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
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.function.BuildableComparableOptionalFunction;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicType;
import org.junit.Test;

import java.util.Optional;
import java.util.function.Predicate;

import static com.terracottatech.store.definition.CellDefinition.defineString;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class StringCellExtractorTest {

  private final StringCellDefinition stringCell = defineString("stringCell");

  @Test
  public void testObjectMethods() {
    StringCellExtractor cellValue = new StringCellExtractor(stringCell);
    assertEquality(cellValue, cellValue);

    StringCellExtractor same = new StringCellExtractor(stringCell);
    assertEquals(cellValue, same);
    assertEquality(cellValue, same);

    StringCellExtractor otherValue = new StringCellExtractor(
            defineString("otherCell"));
    assertInequality(cellValue, otherValue);
  }

  @Test
  public void testLength() {
    Record<?> valueRecord = new TestRecord<>("valueRecord", singleton(stringCell.newCell("value")));

    StringCellExtractor cellValue = new StringCellExtractor(stringCell);

    BuildableComparableOptionalFunction<Record<?>, Integer> length = cellValue.length();
    assertThat(length, is(instanceOf(Intrinsic.class)));
    Intrinsic intrinsic = (Intrinsic) length;
    IntrinsicType intrinsicType = intrinsic.getIntrinsicType();
    assertNotNull(intrinsicType);
    assertEquals(IntrinsicType.OPTIONAL_STRING_LENGTH, intrinsicType);

    Optional<Integer> result = length.apply(valueRecord);
    assertTrue(result.isPresent());
    assertEquals(5, (int) result.get());

    Optional<Integer> emptyResult = length.apply(new TestRecord<>("valueRecord", emptyList()));
    assertFalse(emptyResult.isPresent());

    Predicate<Record<?>> predicate = length.is(5);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertTrue(predicate.test(valueRecord));
    assertFalse(predicate.negate().test(valueRecord));

    assertEquality(length, length);

    StringCellExtractor same = new StringCellExtractor(stringCell);
    BuildableComparableOptionalFunction<Record<?>, Integer> sameLength = same.length();
    assertEquality(length, sameLength);

    StringCellExtractor other = new StringCellExtractor(
            defineString("otherCell"));
    BuildableComparableOptionalFunction<Record<?>, Integer> otherLength = other.length();
    assertInequality(length, otherLength);
  }

  @Test
  public void testStartsWith() {
    StringCellExtractor cellValue = new StringCellExtractor(stringCell);

    BuildablePredicate<Record<?>> startsWith = cellValue.startsWith("prefix");
    assertThat(startsWith, is(instanceOf(Intrinsic.class)));
    Intrinsic intrinsic = (Intrinsic) startsWith;
    IntrinsicType intrinsicType = intrinsic.getIntrinsicType();
    assertNotNull(intrinsicType);
    assertEquals(IntrinsicType.OPTIONAL_STRING_STARTS_WITH, intrinsicType);

    assertTrue(startsWith.test(new TestRecord<>("valueRecord",
            singleton(stringCell.newCell("prefixedValue")))));
    assertFalse(startsWith.test(new TestRecord<>("valueRecord",
            singleton(stringCell.newCell("value")))));
    assertFalse(startsWith.test(new TestRecord<>("valueRecord",
            emptyList())));

    assertEquality(startsWith, startsWith);

    StringCellExtractor same = new StringCellExtractor(stringCell);
    BuildablePredicate<Record<?>> samePrefix = same.startsWith("prefix");
    assertEquality(startsWith, samePrefix);
    BuildablePredicate<Record<?>> otherPrefix = same.startsWith("otherPrefix");
    assertInequality(startsWith, otherPrefix);

    StringCellExtractor other = new StringCellExtractor(
            defineString("other"));
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
