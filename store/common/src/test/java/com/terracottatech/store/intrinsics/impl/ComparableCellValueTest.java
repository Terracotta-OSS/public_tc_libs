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

import org.junit.Test;

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicBuildableComparableFunction;
import com.terracottatech.store.intrinsics.impl.CellValue.ComparableCellValue;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Base test class for {@link ComparableCellValue} implementations.
 */
public abstract class ComparableCellValueTest<T extends Comparable<T>> {

  private final Record<?> nilRecord = new TestRecord<>("nilRecord", emptySet());

  @Test
  public void testAsComparator() throws Exception {
    T expectedCellValue = getCellValue();
    Record<?> valueRecord = getValueRecord(expectedCellValue);
    Record<?> greaterRecord = getValueRecord(getGreaterValue());

    Comparator<Record<?>> comparator = getComparableCellValue(getCellValue()).asComparator();
    // normal
    assertComparison(valueRecord, greaterRecord, comparator);
    // reversed
    assertComparison(greaterRecord, valueRecord, comparator.reversed());
  }

  private void assertComparison(Record<?> valueRecord, Record<?> greaterRecord, Comparator<Record<?>> comparator) {
    assertThat(comparator, is(instanceOf(Intrinsic.class)));

    assertTrue(comparator.compare(valueRecord, valueRecord) == 0);
    assertFalse(comparator.compare(valueRecord, valueRecord) > 0);
    assertFalse(comparator.compare(valueRecord, valueRecord) < 0);

    assertFalse(comparator.compare(valueRecord, greaterRecord) == 0);
    assertFalse(comparator.compare(valueRecord, greaterRecord) > 0);
    assertTrue(comparator.compare(valueRecord, greaterRecord) < 0);

    assertFalse(comparator.compare(greaterRecord, valueRecord) == 0);
    assertTrue(comparator.compare(greaterRecord, valueRecord) > 0);
    assertFalse(comparator.compare(greaterRecord, valueRecord) < 0);

    Comparator<Record<?>> noDefaultComparator = getComparableCellValue(null).asComparator();
    assertThrows(() -> noDefaultComparator.compare(valueRecord, nilRecord), NoSuchElementException.class);
    assertThrows(() -> noDefaultComparator.compare(nilRecord, valueRecord), NoSuchElementException.class);
  }

  @Test
  public void testIs() throws Exception {
    T expectedCellValue = getCellValue();
    Record<?> valueRecord = getValueRecord(expectedCellValue);

    Predicate<Record<?>> predicate = getComparableCellValue(getGreaterValue()).is(expectedCellValue);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertTrue(predicate.test(valueRecord));
    assertFalse(predicate.negate().test(valueRecord));

    assertFalse(predicate.test(nilRecord));
    assertTrue(predicate.negate().test(nilRecord));

    Predicate<Record<?>> noDefaultPredicate = getComparableCellValue(null).is(expectedCellValue);
    assertThat(noDefaultPredicate, is(instanceOf(Intrinsic.class)));
    assertThrows(() -> noDefaultPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> noDefaultPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  @Test
  public void testGreaterThan() throws Exception {
    T expectedCellValue = getCellValue();
    Record<?> valueRecord = getValueRecord(expectedCellValue);

    Predicate<Record<?>> predicate = getComparableCellValue(getGreaterValue()).isGreaterThan(expectedCellValue);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertFalse(predicate.test(valueRecord));
    assertTrue(predicate.negate().test(valueRecord));

    assertTrue(predicate.test(nilRecord));
    assertFalse(predicate.negate().test(nilRecord));

    Predicate<Record<?>> noDefaultPredicate = getComparableCellValue(null).isGreaterThan(expectedCellValue);
    assertThat(noDefaultPredicate, is(instanceOf(Intrinsic.class)));
    assertThrows(() -> noDefaultPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> noDefaultPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  @Test
  public void testGreaterThanOrEqual() throws Exception {
    T expectedCellValue = getCellValue();
    Record<?> valueRecord = getValueRecord(expectedCellValue);

    Predicate<Record<?>> predicate = getComparableCellValue(getGreaterValue()).isGreaterThanOrEqualTo(expectedCellValue);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertTrue(predicate.test(valueRecord));
    assertFalse(predicate.negate().test(valueRecord));

    assertTrue(predicate.test(nilRecord));
    assertFalse(predicate.negate().test(nilRecord));

    Predicate<Record<?>> noDefaultPredicate = getComparableCellValue(null).isGreaterThanOrEqualTo(expectedCellValue);
    assertThat(noDefaultPredicate, is(instanceOf(Intrinsic.class)));
    assertThrows(() -> noDefaultPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> noDefaultPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  @Test
  public void testLessThan() throws Exception {
    T expectedCellValue = getCellValue();
    Record<?> valueRecord = getValueRecord(expectedCellValue);

    Predicate<Record<?>> predicate = getComparableCellValue(getGreaterValue()).isLessThan(expectedCellValue);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertFalse(predicate.test(valueRecord));
    assertTrue(predicate.negate().test(valueRecord));

    assertFalse(predicate.test(nilRecord));
    assertTrue(predicate.negate().test(nilRecord));

    Predicate<Record<?>> noDefaultPredicate = getComparableCellValue(null).isLessThan(expectedCellValue);
    assertThat(noDefaultPredicate, is(instanceOf(Intrinsic.class)));
    assertThrows(() -> noDefaultPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> noDefaultPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  @Test
  public void testLessThanOrEqual() throws Exception {
    T expectedCellValue = getCellValue();
    Record<?> valueRecord = getValueRecord(expectedCellValue);

    Predicate<Record<?>> predicate = getComparableCellValue(getGreaterValue()).isLessThanOrEqualTo(expectedCellValue);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertTrue(predicate.test(valueRecord));
    assertFalse(predicate.negate().test(valueRecord));

    assertFalse(predicate.test(nilRecord));
    assertTrue(predicate.negate().test(nilRecord));

    Predicate<Record<?>> noDefaultPredicate = getComparableCellValue(null).isLessThanOrEqualTo(expectedCellValue);
    assertThat(noDefaultPredicate, is(instanceOf(Intrinsic.class)));
    assertThrows(() -> noDefaultPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> noDefaultPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  protected abstract CellDefinition<T> getCellDefinition();

  protected abstract T getCellValue();

  protected abstract T getGreaterValue();

  Record<?> getValueRecord(T cellValue) {
    return new TestRecord<>("valueRecord", singleton(getCellDefinition().newCell(cellValue)));
  }

  protected abstract IntrinsicBuildableComparableFunction<Record<?>, T> getComparableCellValue(T defaultValue);

  @SuppressWarnings("Duplicates")
  private static <T extends Exception> void assertThrows(Procedure proc, Class<T> expected) {
    try {
      proc.invoke();
      fail("Expecting " + expected.getSimpleName());
    } catch (Exception t) {
      if (!expected.isInstance(t)) {
        throw t;
      }
    }
  }

  @FunctionalInterface
  private interface Procedure {
    void invoke();
  }
}
