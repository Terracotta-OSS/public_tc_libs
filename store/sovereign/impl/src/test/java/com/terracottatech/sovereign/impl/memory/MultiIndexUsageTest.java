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

import com.terracottatech.store.Record;
import org.junit.Test;

import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test complex queries involving multiple indexed cells.
 */
public class MultiIndexUsageTest extends AbstractIndexUsageTest {

  /**
   * Contradictory queries returning empty result set.
   */
  @Test
  public void testContradictions() {
    Predicate<Record<?>> c1gt0 = C1.value().isGreaterThan(0);
    Predicate<Record<?>> c1lt0 = C1.value().isLessThan(0);
    Predicate<Record<?>> c2gt10 = C2.value().isGreaterThan(10);
    Predicate<Record<?>> c3gte20 = C3.value().isGreaterThanOrEqualTo(20);

    try (Stream<Record<String>> recordStream = dataset.records()) {
      //c1 > 0 & c2 > 10 & !(c1 > 0) & c3 >= 20
      long count = recordStream.filter(c1gt0.and(c2gt10).and(c1gt0.negate()).and(c3gte20)).count();
      assertThat(count, is(0L));
    }

    try (Stream<Record<String>> recordStream = dataset.records()) {
      //c1 > 0 & c2 > 10 & c1 < 0 & c3 >= 20
      long count = recordStream.filter(c1gt0.and(c2gt10).and(c1lt0).and(c3gte20)).count();
      assertThat(count, is(0L));
    }
  }

  /**
   * OR query involving two indexed cells.
   */
  @Test
  public void testDisjunctionOfIndexedCells() {
    Predicate<Record<?>> c1gt0 = C1.value().isGreaterThan(0);
    Predicate<Record<?>> c2gt10 = C2.value().isGreaterThan(10);

    //c1 > 0 | c2 > 10
    try (Stream<Record<String>> streamC1Gt0 = dataset.records().filter(c1gt0);
         Stream<Record<String>> streamC2gt10 = dataset.records().filter(c2gt10);
         Stream<Record<String>> streamAnd = dataset.records().filter(c1gt0.and(c2gt10));
         Stream<Record<String>> streamOr = dataset.records().filter(c1gt0.or(c2gt10))) {
      long countC1Gt0 = streamC1Gt0.count();
      long countC2gt10 = streamC2gt10.count();
      long countAnd = streamAnd.count();
      long countOr = streamOr.count();
      assertEquals(countC1Gt0 + countC2gt10 - countAnd, countOr);
    }
  }

  /**
   * OR queries covering the entire dataset.
   */
  @Test
  public void testTautologicalDisjunctions() {
    Predicate<Record<?>> c1gt0 = C1.value().isGreaterThan(0);
    Predicate<Record<?>> c1lt10 = C1.value().isLessThan(10);
    Predicate<Record<?>> c2gt10 = C2.value().isGreaterThan(10);
    Predicate<Record<?>> c2lt20 = C2.value().isLessThan(20);
    Predicate<Record<?>> or = c1gt0.or(c1lt10).or(c2gt10).or(c2lt20);

    //c1 > 0 | c1 < 10 | c2 > 10 | c2 < 20
    try (Stream<Record<String>> filter = dataset.records().filter(or);
         Stream<Record<String>> full = dataset.records()) {
      long filterCount = filter.count();
      long fullCount = full.count();
      assertEquals(fullCount, filterCount);
    }
  }

  /**
   * Test involving AND, OR and negation.
   */
  @Test
  public void testConjunctionOfDisjunctions() {
    Predicate<Record<?>> c1gt0 = C1.value().is(5);
    Predicate<Record<?>> c2gt5 = C2.value().isGreaterThan(5);
    Predicate<Record<?>> or = c1gt0.or(c2gt5).and(c1gt0.or(c2gt5.negate()));

    //(c1 == 5 | c2 > 5) & (c1 == 5 | !(c2 > 5))  => c1 == 5
    try (Stream<Record<String>> filter = dataset.records().filter(or)) {
      long filterCount = filter.count();
      assertEquals(1, filterCount);
    }
  }
}
