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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.intrinsics.Intrinsic;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Basic tests for {@link RecordKey}.
 */
@RunWith(Parameterized.class)
public class RecordKeyTest<K extends Comparable<K>> {

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
            { makeRecord("foo", Cell.cell("junk", 1L)), "zzz" },
            { makeRecord(0L, Cell.cell("junk", 1L)), Long.MAX_VALUE },
            { makeRecord(0, Cell.cell("junk", 1L)), Integer.MAX_VALUE },
            { makeRecord(false, Cell.cell("junk", 1L)), true },
            { makeRecord('A', Cell.cell("junk", 1L)), 'B' },
            { makeRecord(0.0D, Cell.cell("junk", 1L)), Double.MAX_VALUE }
        }
    );
  }

  @Parameterized.Parameter()
  public Record<K> testRecord;

  @Parameterized.Parameter(1)
  public K higherValue;

  private final RecordKey<K> recordKey = new RecordKey<>();

  @Test
  public void testApply() throws Exception {
    assertThat(recordKey.apply(testRecord), is(testRecord.getKey()));
  }

  @Test
  public void testAsComparator() throws Exception {
    Comparator<Record<K>> comparator = recordKey.asComparator();
    assertThat(comparator, is(instanceOf(Intrinsic.class)));
    assertThat(comparator.reversed(), is(instanceOf(Intrinsic.class)));

    @SuppressWarnings("SuspiciousToArrayCall") Cell<?>[] cells = testRecord.toArray(new Cell<?>[0]);
    Record<K> higherKeyRecord = makeRecord(higherValue, cells);
    Record<K> testRecord = this.testRecord;

    // Normal
    assertComparison(comparator, cells, higherKeyRecord, testRecord);
    // Reversed
    assertComparison(comparator.reversed(), cells, testRecord, higherKeyRecord);

  }

  private void assertComparison(Comparator<Record<K>> comparator, Cell<?>[] cells, Record<K> higherKeyRecord, Record<K> testRecord) {
    assertTrue(comparator.compare(testRecord, higherKeyRecord) < 0);
    assertFalse(comparator.compare(testRecord, higherKeyRecord) == 0);
    assertFalse(comparator.compare(testRecord, higherKeyRecord) > 0);

    assertFalse(comparator.compare(higherKeyRecord, testRecord) < 0);
    assertFalse(comparator.compare(higherKeyRecord, testRecord) == 0);
    assertTrue(comparator.compare(higherKeyRecord, testRecord) > 0);

    Record<K> equalKeyRecord = makeRecord(testRecord.getKey(), cells);
    assertFalse(comparator.compare(testRecord, equalKeyRecord) < 0);
    assertTrue(comparator.compare(testRecord, equalKeyRecord) == 0);
    assertFalse(comparator.compare(testRecord, equalKeyRecord) > 0);
  }

  @Test
  public void testIs() throws Exception {
    Predicate<Record<K>> predicate = recordKey.is(higherValue);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertThat(predicate.negate(), is(instanceOf(Intrinsic.class)));

    assertFalse(predicate.test(testRecord));
    assertTrue(predicate.negate().test(testRecord));
    assertTrue(recordKey.is(testRecord.getKey()).test(testRecord));
  }

  @Test
  public void testIsGreaterThan() throws Exception {
    Predicate<Record<K>> predicate = recordKey.isGreaterThan(higherValue);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertThat(predicate.negate(), is(instanceOf(Intrinsic.class)));

    assertFalse(predicate.test(testRecord));
    assertTrue(predicate.negate().test(testRecord));
    assertFalse(recordKey.isGreaterThan(testRecord.getKey()).test(testRecord));
  }

  @Test
  public void testIsLessThan() throws Exception {
    Predicate<Record<K>> predicate = recordKey.isLessThan(higherValue);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertThat(predicate.negate(), is(instanceOf(Intrinsic.class)));

    assertTrue(predicate.test(testRecord));
    assertFalse(predicate.negate().test(testRecord));
    assertFalse(recordKey.isLessThan(testRecord.getKey()).test(testRecord));
  }

  @Test
  public void testIsGreaterThanOrEqualTo() throws Exception {
    Predicate<Record<K>> predicate = recordKey.isGreaterThanOrEqualTo(higherValue);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertThat(predicate.negate(), is(instanceOf(Intrinsic.class)));

    assertFalse(predicate.test(testRecord));
    assertTrue(predicate.negate().test(testRecord));
    assertTrue(recordKey.isGreaterThanOrEqualTo(testRecord.getKey()).test(testRecord));
  }

  @Test
  public void testIsLessThanOrEqualTo() throws Exception {
    Predicate<Record<K>> predicate = recordKey.isLessThanOrEqualTo(higherValue);
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertThat(predicate.negate(), is(instanceOf(Intrinsic.class)));

    assertTrue(predicate.test(testRecord));
    assertFalse(predicate.negate().test(testRecord));
    assertTrue(recordKey.isLessThanOrEqualTo(testRecord.getKey()).test(testRecord));
  }

  private static <K extends Comparable<K>> Record<K> makeRecord(K key, Cell<?>... cells) {
    return new TestRecord<>(key, Arrays.asList(cells));
  }
}
