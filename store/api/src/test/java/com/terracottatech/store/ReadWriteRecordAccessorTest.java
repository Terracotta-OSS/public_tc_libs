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

package com.terracottatech.store;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.terracottatech.store.Cell.cell;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for default methods on {@link ReadWriteRecordAccessor} interface.
 */
public class ReadWriteRecordAccessorTest {

  @Test
  public void testUpsertVarargs() throws Exception {
    TestReadWriteRecordAccessor<String> recordAccessor =
        new TestReadWriteRecordAccessor<>("targetKey",
            Collections.singletonList(
                new TestRecord<>("otherKey", Arrays.asList(cell("one", 2), cell("two", "one")))
            ));

    recordAccessor.upsert(cell("one", 1), cell("two", "two"));
    assertThat(recordAccessor.addVarargCount + recordAccessor.addIterableCount, is(1));
    assertThat(recordAccessor.updateCount, is(0));
    assertTrue(recordAccessor.records.containsValue(new TestRecord<>("targetKey", Arrays.asList(cell("one", 1), cell("two", "two")))));

    recordAccessor.resetCounts();

    recordAccessor.upsert(cell("one", 100), cell("two", "two hundred"));
    assertThat(recordAccessor.addVarargCount + recordAccessor.addIterableCount, is(1));
    assertThat(recordAccessor.updateCount, is(1));
    assertTrue(recordAccessor.records.containsValue(new TestRecord<>("targetKey", Arrays.asList(cell("one", 100), cell("two", "two hundred")))));
  }

  @Test
  public void testUpsertIterable() throws Exception {
    TestReadWriteRecordAccessor<String> recordAccessor =
        new TestReadWriteRecordAccessor<>("targetKey",
            Collections.singletonList(
                new TestRecord<>("otherKey", Arrays.asList(cell("one", 2), cell("two", "one")))
            ));

    recordAccessor.upsert(Arrays.asList(cell("one", 1), cell("two", "two")));
    assertThat(recordAccessor.addVarargCount + recordAccessor.addIterableCount, is(1));
    assertThat(recordAccessor.updateCount, is(0));
    assertTrue(recordAccessor.records.containsValue(new TestRecord<>("targetKey", Arrays.asList(cell("one", 1), cell("two", "two")))));

    recordAccessor.resetCounts();

    recordAccessor.upsert(Arrays.asList(cell("one", 100), cell("two", "two hundred")));
    assertThat(recordAccessor.addVarargCount + recordAccessor.addIterableCount, is(1));
    assertThat(recordAccessor.updateCount, is(1));
    assertTrue(recordAccessor.records.containsValue(new TestRecord<>("targetKey", Arrays.asList(cell("one", 100), cell("two", "two hundred")))));
  }

  @Test
  public void testAddVarargs() throws Exception {
    TestReadWriteRecordAccessor<String> recordAccessor =
        new TestReadWriteRecordAccessor<>("targetKey",
            Collections.singletonList(
                new TestRecord<>("otherKey", Arrays.asList(cell("one", 2), cell("two", "one")))
            ));

    TestRecord<String> expectedRecord = new TestRecord<>("targetKey", Arrays.asList(cell("one", 1), cell("two", "two")));

    assertThat(recordAccessor.add(cell("one", 1), cell("two", "two")), is(Optional.empty()));
    assertThat(recordAccessor.addVarargCount, is(1));
    assertThat(recordAccessor.addIterableCount, is(0));
    assertThat(recordAccessor.updateCount, is(0));
    assertTrue(recordAccessor.records.containsValue(expectedRecord));

    recordAccessor.resetCounts();

    assertThat(recordAccessor.add(cell("one", 100), cell("two", "two hundred")), is(Optional.of(expectedRecord)));
    assertThat(recordAccessor.addVarargCount, is(1));
    assertThat(recordAccessor.addIterableCount, is(0));
    assertThat(recordAccessor.updateCount, is(0));
    assertFalse(recordAccessor.records.containsValue(new TestRecord<>("targetKey", Arrays.asList(cell("one", 100), cell("two", "two hundred")))));
  }

  @Test
  public void testAddIterable() throws Exception {
    TestReadWriteRecordAccessor<String> recordAccessor =
        new TestReadWriteRecordAccessor<>("targetKey",
            Collections.singletonList(
                new TestRecord<>("otherKey", Arrays.asList(cell("one", 2), cell("two", "one")))
            ));

    TestRecord<String> expectedRecord = new TestRecord<>("targetKey", Arrays.asList(cell("one", 1), cell("two", "two")));

    assertThat(recordAccessor.add(Arrays.asList(cell("one", 1), cell("two", "two"))), is(Optional.empty()));
    assertThat(recordAccessor.addVarargCount, is(0));
    assertThat(recordAccessor.addIterableCount, is(1));
    assertThat(recordAccessor.updateCount, is(0));
    assertTrue(recordAccessor.records.containsValue(expectedRecord));

    recordAccessor.resetCounts();

    assertThat(recordAccessor.add(Arrays.asList(cell("one", 100), cell("two", "two hundred"))), is(Optional.of(expectedRecord)));
    assertThat(recordAccessor.addVarargCount, is(0));
    assertThat(recordAccessor.addIterableCount, is(1));
    assertThat(recordAccessor.updateCount, is(0));
    assertFalse(recordAccessor.records.containsValue(new TestRecord<>("targetKey", Arrays.asList(cell("one", 100), cell("two", "two hundred")))));
  }


  private static class TestReadWriteRecordAccessor<K extends Comparable<K>> implements ReadWriteRecordAccessor<K> {

    private int addVarargCount = 0;
    private int addIterableCount = 0;
    private int updateCount = 0;

    private final K key;
    private final Map<K, Record<K>> records;

    TestReadWriteRecordAccessor(K key, List<Record<K>> records) {
      this.key = key;
      Map<K, Record<K>> recordMap = new HashMap<>();
      records.forEach((r) -> recordMap.put(r.getKey(), r));
      this.records = recordMap;
    }

    void resetCounts() {
      addVarargCount = 0;
      addIterableCount = 0;
      updateCount = 0;
    }

    @Override
    public ConditionalReadWriteRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Record<K>> add(Cell<?>... cells) {
      addVarargCount++;
      return addCommon(Arrays.asList(cells));
    }

    @Override
    public Optional<Record<K>> add(Iterable<Cell<?>> cells) {
      addIterableCount++;
      return addCommon(cells);
    }

    private Optional<Record<K>> addCommon(Iterable<Cell<?>> cells) {
      Record<K> record = records.get(key);
      if (record == null) {
        record = new TestRecord<>(key, cells);
        records.put(key, record);
        return Optional.empty();
      }
      return Optional.of(record);
    }

    @Override
    public Optional<Tuple<Record<K>, Record<K>>> update(UpdateOperation<? super K> transform) {
      updateCount++;
      Record<K> oldRecord = records.get(key);
      if (oldRecord != null) {
        @SuppressWarnings("unchecked")
        Record<K> newRecord = new TestRecord<>(key, transform.apply((Record) oldRecord));
        records.put(key, newRecord);
        return Optional.of(Tuple.of(oldRecord, newRecord));
      }
      return Optional.empty();
    }

    @Override
    public Optional<Record<K>> delete() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> Optional<T> read(Function<? super Record<K>, T> mapper) {
      throw new UnsupportedOperationException();
    }
  }

}