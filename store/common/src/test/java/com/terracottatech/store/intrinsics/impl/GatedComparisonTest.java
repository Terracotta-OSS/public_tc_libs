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
import com.terracottatech.store.intrinsics.IntrinsicFunction;

import java.util.Optional;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Basic tests for {@link GatedComparison}.
 */
public class GatedComparisonTest {

  private final LongCellDefinition longCell = CellDefinition.defineLong("longCell");
  @SuppressWarnings("unchecked")
  private final IntrinsicFunction<Record<?>, Optional<Long>> longCellExtractor =
      (IntrinsicFunction<Record<?>, Optional<Long>>)CellExtractor.extractComparable(longCell);
  private final Record<String> oneRecord = new TestRecord<>("oneRecord", singleton(longCell.newCell(1L)));
  private final Record<String> zeroRecord = new TestRecord<>("zeroRecord", singleton(longCell.newCell(0L)));
  private final Record<String> nilRecord = new TestRecord<>("nilRecord", emptySet());

  @Test
  public void testEquals() throws Exception {
    GatedComparison.Equals<Record<?>, Long> predicate =
        new GatedComparison.Equals<>(longCellExtractor, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(longCell==1)"));
    assertThat(predicate.negate().toString(), is("(!(longCell==1))"));

    assertTrue(predicate.test(oneRecord));
    assertFalse(predicate.negate().test(oneRecord));

    assertFalse(predicate.test(nilRecord));
    assertFalse(predicate.negate().test(nilRecord));

    predicate = new GatedComparison.Equals<>(longCellExtractor, new Constant<>(1L));
    assertFalse(predicate.test(zeroRecord));
    assertTrue(predicate.negate().test(zeroRecord));

    predicate = new GatedComparison.Equals<>(longCellExtractor, new Constant<>(0L));
    assertFalse(predicate.test(oneRecord));
    assertTrue(predicate.negate().test(oneRecord));
  }

  @Test
  public void testContrastEq() throws Exception {
    GatedComparison.Contrast<Record<?>, Long> predicate =
        new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.EQ, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(longCell==1)"));
    assertThat(predicate.negate().toString(), is("(longCell!=1)"));

    assertTrue(predicate.test(oneRecord));
    assertFalse(predicate.negate().test(oneRecord));

    assertFalse(predicate.test(nilRecord));
    assertFalse(predicate.negate().test(nilRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.EQ, new Constant<>(1L));
    assertFalse(predicate.test(zeroRecord));
    assertTrue(predicate.negate().test(zeroRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.EQ, new Constant<>(0L));
    assertFalse(predicate.test(oneRecord));
    assertTrue(predicate.negate().test(oneRecord));
  }

  @Test
  public void testContrastNeq() throws Exception {
    GatedComparison.Contrast<Record<?>, Long> predicate =
        new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.NEQ, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(longCell!=1)"));
    assertThat(predicate.negate().toString(), is("(longCell==1)"));

    assertFalse(predicate.test(oneRecord));
    assertTrue(predicate.negate().test(oneRecord));

    assertFalse(predicate.test(nilRecord));
    assertFalse(predicate.negate().test(nilRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.NEQ, new Constant<>(1L));
    assertTrue(predicate.test(zeroRecord));
    assertFalse(predicate.negate().test(zeroRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.NEQ, new Constant<>(0L));
    assertTrue(predicate.test(oneRecord));
    assertFalse(predicate.negate().test(oneRecord));
  }

  @Test
  public void testContrastGreaterThan() throws Exception {
    GatedComparison.Contrast<Record<?>, Long> predicate =
        new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.GREATER_THAN, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(longCell>1)"));
    assertThat(predicate.negate().toString(), is("(longCell<=1)"));

    assertFalse(predicate.test(zeroRecord));
    assertTrue(predicate.negate().test(zeroRecord));

    assertFalse(predicate.test(nilRecord));
    assertFalse(predicate.negate().test(nilRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.GREATER_THAN, new Constant<>(0L));
    assertTrue(predicate.test(oneRecord));
    assertFalse(predicate.negate().test(oneRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.GREATER_THAN, new Constant<>(0L));
    assertFalse(predicate.test(zeroRecord));
    assertTrue(predicate.negate().test(zeroRecord));
  }

  @Test
  public void testContrastGreaterThanOrEqual() throws Exception {
    GatedComparison.Contrast<Record<?>, Long> predicate =
        new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.GREATER_THAN_OR_EQUAL, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(longCell>=1)"));
    assertThat(predicate.negate().toString(), is("(longCell<1)"));

    assertFalse(predicate.test(zeroRecord));
    assertTrue(predicate.negate().test(zeroRecord));

    assertFalse(predicate.test(nilRecord));
    assertFalse(predicate.negate().test(nilRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.GREATER_THAN_OR_EQUAL, new Constant<>(0L));
    assertTrue(predicate.test(oneRecord));
    assertFalse(predicate.negate().test(oneRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.GREATER_THAN_OR_EQUAL, new Constant<>(0L));
    assertTrue(predicate.test(zeroRecord));
    assertFalse(predicate.negate().test(zeroRecord));
  }

  @Test
  public void testContrastLessThan() throws Exception {
    GatedComparison.Contrast<Record<?>, Long> predicate =
        new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.LESS_THAN, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(longCell<1)"));
    assertThat(predicate.negate().toString(), is("(longCell>=1)"));

    assertTrue(predicate.test(zeroRecord));
    assertFalse(predicate.negate().test(zeroRecord));

    assertFalse(predicate.test(nilRecord));
    assertFalse(predicate.negate().test(nilRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.LESS_THAN, new Constant<>(0L));
    assertFalse(predicate.test(oneRecord));
    assertTrue(predicate.negate().test(oneRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.LESS_THAN, new Constant<>(0L));
    assertFalse(predicate.test(zeroRecord));
    assertTrue(predicate.negate().test(zeroRecord));
  }

  @Test
  public void testContrastLessThanOrEqual() throws Exception {
    GatedComparison.Contrast<Record<?>, Long> predicate =
        new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.LESS_THAN_OR_EQUAL, new Constant<>(1L));
    assertThat(predicate, instanceOf(Intrinsic.class));
    assertThat(predicate.negate(), instanceOf(Intrinsic.class));
    assertThat(predicate.toString(), is("(longCell<=1)"));
    assertThat(predicate.negate().toString(), is("(longCell>1)"));

    assertTrue(predicate.test(zeroRecord));
    assertFalse(predicate.negate().test(zeroRecord));

    assertFalse(predicate.test(nilRecord));
    assertFalse(predicate.negate().test(nilRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.LESS_THAN_OR_EQUAL, new Constant<>(0L));
    assertFalse(predicate.test(oneRecord));
    assertTrue(predicate.negate().test(oneRecord));

    predicate = new GatedComparison.Contrast<>(longCellExtractor, ComparisonType.LESS_THAN_OR_EQUAL, new Constant<>(0L));
    assertTrue(predicate.test(zeroRecord));
    assertFalse(predicate.negate().test(zeroRecord));
  }
}