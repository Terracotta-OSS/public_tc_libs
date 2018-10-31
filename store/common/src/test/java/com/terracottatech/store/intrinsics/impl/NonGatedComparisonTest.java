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
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.intrinsics.Intrinsic;

import java.util.NoSuchElementException;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Basic tests for {@link NonGatedComparison}.
 */
public class NonGatedComparisonTest {

  private final LongCellDefinition longCell = CellDefinition.defineLong("longCell");
  private final CellValue.LongCellValue longCellValue = new CellValue.LongCellValue(longCell, null);
  private final Record<String> nilRecord = new TestRecord<>("nilRecord", emptySet());

  @Test
  public void testEquals() throws Exception {
    NonGatedComparison.Equals<String, Long> predicate =
        new NonGatedComparison.Equals<>(new Constant<>(1L), new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(1==1)"));
    assertThat(predicate.negate().toString(), is("(!(1==1))"));

    assertTrue(predicate.test("foo"));
    assertFalse(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Equals<>(new Constant<>(0L), new Constant<>(1L));
    assertFalse(predicate.test("foo"));
    assertTrue(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Equals<>(new Constant<>(1L), new Constant<>(0L));
    assertFalse(predicate.test("foo"));
    assertTrue(predicate.negate().test("foo"));

    NonGatedComparison.Equals<Record<?>, Long> recordPredicate =
        new NonGatedComparison.Equals<>(longCellValue, new Constant<>(1L));
    assertThrows(() -> recordPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> recordPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  @Test
  public void testContrastEq() throws Exception {
    NonGatedComparison.Contrast<String, Long> predicate =
        new NonGatedComparison.Contrast<>(new Constant<>(1L), ComparisonType.EQ, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(1==1)"));
    assertThat(predicate.negate().toString(), is("(1!=1)"));

    assertTrue(predicate.test("foo"));
    assertFalse(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(0L), ComparisonType.EQ, new Constant<>(1L));
    assertFalse(predicate.test("foo"));
    assertTrue(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(1L), ComparisonType.EQ, new Constant<>(0L));
    assertFalse(predicate.test("foo"));
    assertTrue(predicate.negate().test("foo"));

    NonGatedComparison.Contrast<Record<?>, Long> recordPredicate =
        new NonGatedComparison.Contrast<>(longCellValue, ComparisonType.EQ, new Constant<>(1L));
    assertThrows(() -> recordPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> recordPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  @Test
  public void testContrastNeq() throws Exception {
    NonGatedComparison.Contrast<String, Long> predicate =
        new NonGatedComparison.Contrast<>(new Constant<>(1L), ComparisonType.NEQ, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(1!=1)"));
    assertThat(predicate.negate().toString(), is("(1==1)"));

    assertFalse(predicate.test("foo"));
    assertTrue(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(0L), ComparisonType.NEQ, new Constant<>(1L));
    assertTrue(predicate.test("foo"));
    assertFalse(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(1L), ComparisonType.NEQ, new Constant<>(0L));
    assertTrue(predicate.test("foo"));
    assertFalse(predicate.negate().test("foo"));

    NonGatedComparison.Contrast<Record<?>, Long> recordPredicate =
        new NonGatedComparison.Contrast<>(longCellValue, ComparisonType.NEQ, new Constant<>(1L));
    assertThrows(() -> recordPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> recordPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  @Test
  public void testContrastGreaterThan() throws Exception {
    NonGatedComparison.Contrast<String, Long> predicate =
        new NonGatedComparison.Contrast<>(new Constant<>(0L), ComparisonType.GREATER_THAN, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(0>1)"));
    assertThat(predicate.negate().toString(), is("(0<=1)"));

    assertFalse(predicate.test("foo"));
    assertTrue(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(1L), ComparisonType.GREATER_THAN, new Constant<>(0L));
    assertTrue(predicate.test("foo"));
    assertFalse(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(0L), ComparisonType.GREATER_THAN, new Constant<>(0L));
    assertFalse(predicate.test("foo"));
    assertTrue(predicate.negate().test("foo"));

    NonGatedComparison.Contrast<Record<?>, Long> recordPredicate =
        new NonGatedComparison.Contrast<>(longCellValue, ComparisonType.GREATER_THAN, new Constant<>(1L));
    assertThrows(() -> recordPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> recordPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  @Test
  public void testContrastGreaterThanOrEqual() throws Exception {
    NonGatedComparison.Contrast<String, Long> predicate =
        new NonGatedComparison.Contrast<>(new Constant<>(0L), ComparisonType.GREATER_THAN_OR_EQUAL, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(0>=1)"));
    assertThat(predicate.negate().toString(), is("(0<1)"));

    assertFalse(predicate.test("foo"));
    assertTrue(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(1L), ComparisonType.GREATER_THAN_OR_EQUAL, new Constant<>(0L));
    assertTrue(predicate.test("foo"));
    assertFalse(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(0L), ComparisonType.GREATER_THAN_OR_EQUAL, new Constant<>(0L));
    assertTrue(predicate.test("foo"));
    assertFalse(predicate.negate().test("foo"));

    NonGatedComparison.Contrast<Record<?>, Long> recordPredicate =
        new NonGatedComparison.Contrast<>(longCellValue, ComparisonType.GREATER_THAN_OR_EQUAL, new Constant<>(1L));
    assertThrows(() -> recordPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> recordPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  @Test
  public void testContrastLessThan() throws Exception {
    NonGatedComparison.Contrast<String, Long> predicate =
        new NonGatedComparison.Contrast<>(new Constant<>(0L), ComparisonType.LESS_THAN, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(0<1)"));
    assertThat(predicate.negate().toString(), is("(0>=1)"));

    assertTrue(predicate.test("foo"));
    assertFalse(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(1L), ComparisonType.LESS_THAN, new Constant<>(0L));
    assertFalse(predicate.test("foo"));
    assertTrue(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(0L), ComparisonType.LESS_THAN, new Constant<>(0L));
    assertFalse(predicate.test("foo"));
    assertTrue(predicate.negate().test("foo"));

    NonGatedComparison.Contrast<Record<?>, Long> recordPredicate =
        new NonGatedComparison.Contrast<>(longCellValue, ComparisonType.LESS_THAN, new Constant<>(1L));
    assertThrows(() -> recordPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> recordPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  @Test
  public void testContrastLessThanOrEqual() throws Exception {
    NonGatedComparison.Contrast<String, Long> predicate =
        new NonGatedComparison.Contrast<>(new Constant<>(0L), ComparisonType.LESS_THAN_OR_EQUAL, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(0<=1)"));
    assertThat(predicate.negate().toString(), is("(0>1)"));

    assertTrue(predicate.test("foo"));
    assertFalse(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(1L), ComparisonType.LESS_THAN_OR_EQUAL, new Constant<>(0L));
    assertFalse(predicate.test("foo"));
    assertTrue(predicate.negate().test("foo"));

    predicate = new NonGatedComparison.Contrast<>(new Constant<>(0L), ComparisonType.LESS_THAN_OR_EQUAL, new Constant<>(0L));
    assertTrue(predicate.test("foo"));
    assertFalse(predicate.negate().test("foo"));

    NonGatedComparison.Contrast<Record<?>, Long> recordPredicate =
        new NonGatedComparison.Contrast<>(longCellValue, ComparisonType.LESS_THAN_OR_EQUAL, new Constant<>(1L));
    assertThrows(() -> recordPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> recordPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

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