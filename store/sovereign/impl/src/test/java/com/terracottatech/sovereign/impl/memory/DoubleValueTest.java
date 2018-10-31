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

package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Stream;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.junit.Assert.assertEquals;


/**
 * Test correctness of queries involving Doubles, including special values.
 * Note: primitive comparisons in lambda expressions with unboxed values behave differently from
 * DSL comparison methods for NaN. The former conform with IEEE 754 standard, whereas the latter
 * follow the {@link java.lang.Double} specification of equals and compareTo methods.
 */
public class DoubleValueTest {

  private static final DoubleCellDefinition D = CellDefinition.defineDouble("double1");

  private SovereignDataset<String> dataset = null;
  private long total;

  @Before
  public void before() throws Exception {
    dataset = new SovereignBuilder<>(Type.STRING, FixedTimeReference.class).offheap(128 * 1024 * 1024).limitVersionsTo(1).build();
    for (double i = 0; i < 10; i++) {
      dataset.add(IMMEDIATE, "key_" + i, D.newCell(i));
    }
    dataset.add(IMMEDIATE, "key_Neg_Inf", D.newCell(NEGATIVE_INFINITY));
    dataset.add(IMMEDIATE, "key_Pos_Inf", D.newCell(POSITIVE_INFINITY));
    dataset.add(IMMEDIATE, "key_NaN", D.newCell(NaN));
    dataset.getIndexing().createIndex(D, SovereignIndexSettings.BTREE).call();

    try (Stream<Record<String>> recordStream = dataset.records()) {
      total = recordStream.count();
    }
  }

  @After
  public void after() throws IOException {
    dataset.getStorage().destroyDataSet(dataset.getUUID());
    dataset = null;
    System.gc();
  }

  /**
   * NaN > any other value, including POSITIVE_INFINITY.
   */
  @Test
  public void testNaN() {
    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(1, recordStream.filter(D.value().is(NaN)).count());
      assertEquals(1, boxedLambdaStream.filter(r -> getValue(r).compareTo(NaN) == 0).count());
      assertEquals(0, unboxedLambdaStream.filter(r -> getValue(r) == NaN).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(total, recordStream.filter(D.value().isLessThanOrEqualTo(NaN)).count());
      assertEquals(total, boxedLambdaStream.filter(r -> getValue(r).compareTo(NaN) <= 0).count());
      assertEquals(0, unboxedLambdaStream.filter(r -> getValue(r) <= NaN).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(total - 1, recordStream.filter(D.value().isLessThan(NaN)).count());
      assertEquals(total - 1, boxedLambdaStream.filter(r -> getValue(r).compareTo(NaN) < 0).count());
      assertEquals(0, unboxedLambdaStream.filter(r -> getValue(r) < NaN).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(1, recordStream.filter(D.value().isGreaterThanOrEqualTo(NaN)).count());
      assertEquals(1, boxedLambdaStream.filter(r -> getValue(r).compareTo(NaN) >= 0).count());
      assertEquals(0, unboxedLambdaStream.filter(r -> getValue(r) >= NaN).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(0, recordStream.filter(D.value().isGreaterThan(NaN)).count());
      assertEquals(0, boxedLambdaStream.filter(r -> getValue(r).compareTo(NaN) > 0).count());
      assertEquals(0, unboxedLambdaStream.filter(r -> getValue(r) > NaN).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(total - 1, recordStream.filter(D.value().is(NaN).negate()).count());
      assertEquals(total - 1, boxedLambdaStream.filter(r -> getValue(r).compareTo(NaN) != 0).count());
      assertEquals(total, unboxedLambdaStream.filter(r -> getValue(r) != NaN).count());
    }
  }


  /**
   * POSITIVE_INFINITY < NaN, and > any other value.
   */
  @Test
  public void testPositiveInfinity() {
    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(1, recordStream.filter(D.value().is(POSITIVE_INFINITY)).count());
      assertEquals(1, boxedLambdaStream.filter(r -> getValue(r).compareTo(POSITIVE_INFINITY) == 0).count());
      assertEquals(1, unboxedLambdaStream.filter(r -> getValue(r) == POSITIVE_INFINITY).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(total - 1, recordStream.filter(D.value().isLessThanOrEqualTo(POSITIVE_INFINITY)).count());
      assertEquals(total - 1, boxedLambdaStream.filter(r -> getValue(r).compareTo(POSITIVE_INFINITY) <= 0).count());
      assertEquals(total - 1, unboxedLambdaStream.filter(r -> getValue(r) <= POSITIVE_INFINITY).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(total - 2, recordStream.filter(D.value().isLessThan(POSITIVE_INFINITY)).count());
      assertEquals(total - 2, boxedLambdaStream.filter(r -> getValue(r).compareTo(POSITIVE_INFINITY) < 0).count());
      assertEquals(total - 2, unboxedLambdaStream.filter(r -> getValue(r) < POSITIVE_INFINITY).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(2, recordStream.filter(D.value().isGreaterThanOrEqualTo(POSITIVE_INFINITY)).count());
      assertEquals(2, boxedLambdaStream.filter(r -> getValue(r).compareTo(POSITIVE_INFINITY) >= 0).count());
      assertEquals(1, unboxedLambdaStream.filter(r -> getValue(r) >= POSITIVE_INFINITY).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(1, recordStream.filter(D.value().isGreaterThan(POSITIVE_INFINITY)).count());
      assertEquals(1, boxedLambdaStream.filter(r -> getValue(r).compareTo(POSITIVE_INFINITY) > 0).count());
      assertEquals(0, unboxedLambdaStream.filter(r -> getValue(r) > POSITIVE_INFINITY).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(total - 1, recordStream.filter(D.value().is(POSITIVE_INFINITY).negate()).count());
      assertEquals(total - 1, boxedLambdaStream.filter(r -> getValue(r).compareTo(POSITIVE_INFINITY) != 0).count());
      assertEquals(total - 1, unboxedLambdaStream.filter(r -> getValue(r) != POSITIVE_INFINITY).count());
    }
  }

  /**
   * NEGATIVE_INFINITY < any other value.
   */
  @Test
  public void testNegativeInfinity() {
    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(1, recordStream.filter(D.value().is(NEGATIVE_INFINITY)).count());
      assertEquals(1, boxedLambdaStream.filter(r -> getValue(r).compareTo(NEGATIVE_INFINITY) == 0).count());
      assertEquals(1, unboxedLambdaStream.filter(r -> getValue(r) == NEGATIVE_INFINITY).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(1, recordStream.filter(D.value().isLessThanOrEqualTo(NEGATIVE_INFINITY)).count());
      assertEquals(1, boxedLambdaStream.filter(r -> getValue(r).compareTo(NEGATIVE_INFINITY) <= 0).count());
      assertEquals(1, unboxedLambdaStream.filter(r -> getValue(r) <= NEGATIVE_INFINITY).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(0, recordStream.filter(D.value().isLessThan(NEGATIVE_INFINITY)).count());
      assertEquals(0, boxedLambdaStream.filter(r -> getValue(r).compareTo(NEGATIVE_INFINITY) < 0).count());
      assertEquals(0, unboxedLambdaStream.filter(r -> getValue(r) < NEGATIVE_INFINITY).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(total, recordStream.filter(D.value().isGreaterThanOrEqualTo(NEGATIVE_INFINITY)).count());
      assertEquals(total, boxedLambdaStream.filter(r -> getValue(r).compareTo(NEGATIVE_INFINITY) >= 0).count());
      assertEquals(total - 1, unboxedLambdaStream.filter(r -> getValue(r) >= NEGATIVE_INFINITY).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(total - 1, recordStream.filter(D.value().isGreaterThan(NEGATIVE_INFINITY)).count());
      assertEquals(total - 1, boxedLambdaStream.filter(r -> getValue(r).compareTo(NEGATIVE_INFINITY) > 0).count());
      assertEquals(total - 2, unboxedLambdaStream.filter(r -> getValue(r) > NEGATIVE_INFINITY).count());
    }

    try (Stream<Record<String>> recordStream = dataset.records();
         Stream<Record<String>> boxedLambdaStream = dataset.records();
         Stream<Record<String>> unboxedLambdaStream = dataset.records()) {
      assertEquals(total - 1, recordStream.filter(D.value().is(NEGATIVE_INFINITY).negate()).count());
      assertEquals(total - 1, boxedLambdaStream.filter(r -> getValue(r).compareTo(NEGATIVE_INFINITY) != 0).count());
      assertEquals(total - 1, unboxedLambdaStream.filter(r -> getValue(r) != NEGATIVE_INFINITY).count());
    }
  }

  private Double getValue(Record<String> record) {
    return record.get(D).orElse(Double.MIN_VALUE);
  }
}
