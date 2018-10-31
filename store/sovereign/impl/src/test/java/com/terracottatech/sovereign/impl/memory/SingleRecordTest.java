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
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReference;

import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;

import static com.terracottatech.store.Cell.cell;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by cschanck on 7/28/2015.
 */
public class SingleRecordTest {

  private static final SystemTimeReference.Generator TIME_REFERENCE_GENERATOR = new SystemTimeReference.Generator();

  private static final Cell<String> STRING_CELL = cell("stringCell", "deadbeef");
  private static final Cell<Integer> INT_CELL = cell("intCell", 1776);
  private static final Cell<Long> LONG_CELL = cell("longCell", BigInteger.valueOf(2).pow(63).subtract(BigInteger.valueOf(25)).longValue());
  private static final Cell<Double> DOUBLE_CELL = cell("doubleCell", Math.PI);
  private static final Cell<byte[]> BYTES_CELL = cell("bytesCell", new byte[] { (byte)0xCA, (byte)0xFE, (byte)0xBA, (byte)0xBE });
  private static final Cell<Boolean> BOOL_CELL = cell("boolCell", false);

  private SystemTimeReference lastTime = TIME_REFERENCE_GENERATOR.get();

  @Test
  public void testCtorArray() throws Exception {
    final SystemTimeReference originTime = getUniqueTime();
    final long msn = 8675309L;
    final String key = "ORIGINAL";

    final SingleRecord<String> original =
        new SingleRecord<>(null, key, originTime, msn,
            BYTES_CELL, INT_CELL, DOUBLE_CELL, BOOL_CELL, STRING_CELL, LONG_CELL);

    assertThat(original.getKey(), is(key));
    assertThat(original.getTimeReference(), is(originTime));
    assertThat(original.getMSN(), is(msn));
    assertThat(original, containsInAnyOrder(BYTES_CELL, INT_CELL, DOUBLE_CELL, BOOL_CELL, STRING_CELL, LONG_CELL));
  }

  @Test
  public void testCtorIterable() throws Exception {
    final SystemTimeReference originTime = getUniqueTime();
    final long msn = 8675309L;
    final String key = "ORIGINAL";

    final SingleRecord<String> original =
        new SingleRecord<>(null, key, originTime, msn,
            CellSet.of(BYTES_CELL, INT_CELL, DOUBLE_CELL, BOOL_CELL, STRING_CELL, LONG_CELL));

    assertThat(original.getKey(), is(key));
    assertThat(original.getTimeReference(), is(originTime));
    assertThat(original.getMSN(), is(msn));
    assertThat(original, containsInAnyOrder(BYTES_CELL, INT_CELL, DOUBLE_CELL, BOOL_CELL, STRING_CELL, LONG_CELL));
  }

  @Test
  public void testGetOptional() {
    SovereignDataset<String> ds = new SovereignBuilder<>(Type.STRING, FixedTimeReference.class).heap().build();
    ds.add(SovereignDataset.Durability.IMMEDIATE, "foo", cell("age",10l));
    Record<String> record = ds.records().findFirst().get();
    Optional<Integer> opt = record.get(CellDefinition.define("age", Type.INT));
    assertThat(opt.isPresent(), is(false));
  }

  @Test
  public void testDeepEquals() throws Exception {
    final SystemTimeReference originTime = getUniqueTime();
    SystemTimeReference alteredTime = getUniqueTime();

    final long msn = 8675309L;
    final String key = "ORIGINAL";

    final SingleRecord<String> original =
        new SingleRecord<>(null, key, originTime, msn,
            cell("a", 1), cell("b", "bee"), cell("c", 299_792_458L), cell("d", new byte[] {0, 1, 2}));
    assertThat(original.deepEquals(original), is(true));

    final SingleRecord<String> replicant =
        new SingleRecord<>(null, key, originTime, msn,
            cell("c", 299_792_458L), cell("a", 1), cell("b", "bee"), cell("d", new byte[] {0, 1, 2}));
    assertThat(original.deepEquals(replicant), is(true));
    assertThat(replicant.deepEquals(original), is(true));

    final SingleRecord<String> timeReferenceAltered =
        new SingleRecord<>(null, key, alteredTime, msn,
            cell("d", new byte[] {0, 1, 2}), cell("c", 299_792_458L), cell("a", 1), cell("b", "bee"));
    assertThat(original.deepEquals(timeReferenceAltered), is(false));
    assertThat(timeReferenceAltered.deepEquals(original), is(false));

    final SingleRecord<String> cellAltered =
        new SingleRecord<>(null, key, originTime, msn,
            cell("c", 299_792_458L), cell("a", 2), cell("d", new byte[] {0, 1, 2}), cell("b", "bee"));
    assertThat(original.deepEquals(cellAltered), is(false));
    assertThat(cellAltered.deepEquals(original), is(false));

    final SingleRecord<String> differentCell =
        new SingleRecord<>(null, key, originTime, msn,
            cell("c", 299_792_458L), cell("d", new byte[] {0, 1, 2}), cell("e", 42), cell("b", "bee"));
    assertThat(original.deepEquals(differentCell), is(false));
    assertThat(differentCell.deepEquals(original), is(false));
  }

  @Test
  public void testIterator() throws Exception {
    SingleRecord<String> record = getTestRecord();
    Iterator<Cell<?>> iterator = record.iterator();
    assertThat(iterator, is(notNullValue()));

    assertTrue(iterator.hasNext());

    List<Cell<?>> expectedCells = new ArrayList<>(getTestCells());
    assertTrue(expectedCells.remove(iterator.next()));
    assertThrows(iterator::remove, UnsupportedOperationException.class);

    iterator.forEachRemaining(expectedCells::remove);
    assertTrue(expectedCells.isEmpty());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testForEachConsumer() throws Exception {
    SingleRecord<String> record = getTestRecord();

    List<Cell<?>> expectedCells = new ArrayList<>(getTestCells());
    record.forEach(expectedCells::remove);
    assertTrue(expectedCells.isEmpty());
  }

  @Test
  public void testSpliterator() throws Exception {
    Collection<Cell<?>> expectedCells = new ArrayList<>(getTestCells());
    SingleRecord<String> record = getTestRecord();
    Spliterator<Cell<?>> spliterator = record.spliterator();

    assertTrue((spliterator.characteristics() & (Spliterator.CONCURRENT | Spliterator.ORDERED | Spliterator.SORTED)) == 0);
    assertTrue(spliterator.hasCharacteristics(Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.SIZED | Spliterator.SUBSIZED));

    assertThat(spliterator.estimateSize(), is((long)expectedCells.size()));
    assertThat(spliterator.getExactSizeIfKnown(), is((long)expectedCells.size()));

    assertThrows(() -> spliterator.tryAdvance(null), NullPointerException.class);
    assertTrue(spliterator.tryAdvance(expectedCells::remove));

    spliterator.forEachRemaining(expectedCells::remove);
    assertTrue(expectedCells.isEmpty());

    assertFalse(spliterator.tryAdvance((c) -> { throw new AssertionError("unexpected Consumer call"); }));
  }

  @Test
  public void testAdd() throws Exception {
    SingleRecord<String> record = getTestRecord();
    assertThrows(() -> record.add(null), UnsupportedOperationException.class);
    assertThrows(() -> record.add(cell("another", "cafebabe")), UnsupportedOperationException.class);
  }

  @Test
  public void testAddAll() throws Exception {
    SingleRecord<String> record = getTestRecord();
    assertThrows(() -> record.addAll(null), UnsupportedOperationException.class);
    assertThrows(() -> record.addAll(Arrays.asList(cell("another", "cafebabe"), cell("additional", 1))), UnsupportedOperationException.class);
  }

  @Test
  public void testClear() throws Exception {
    SingleRecord<String> record = getTestRecord();
    assertThrows(record::clear, UnsupportedOperationException.class);
  }

  @Test
  public void testContains() throws Exception {
    SingleRecord<String> record = getTestRecord();

    assertThrows(() -> record.contains(null), NullPointerException.class);
    assertTrue(record.contains(INT_CELL));
    assertFalse(record.contains(INT_CELL.definition().newCell(5551212)));
  }

  @Test
  public void testContainsAll() throws Exception {
    SingleRecord<String> record = getTestRecord();

    assertThrows(() -> record.containsAll(null), NullPointerException.class);
    assertTrue(record.containsAll(Arrays.asList(STRING_CELL, INT_CELL)));
    assertFalse(record.containsAll(Arrays.asList(STRING_CELL, INT_CELL.definition().newCell(5551212))));

    assertThrows(() -> record.containsAll(Collections.singleton(null)), NullPointerException.class);
  }

  /**
   * Tests the {@link SingleRecord#equals(Object)} contract.  Note that the contract specifies that only
   * the key and MSN are compared.
   */
  @Test
  public void testEquals() throws Exception {
    SingleRecord<String> record =
        new SingleRecord<>(null, "recordKey", getUniqueTime(), 8675309L, getTestCells());
    SingleRecord<String> identicalRecord =
        new SingleRecord<>(null, "recordKey", getUniqueTime(), 8675309L, getTestCells());
    SingleRecord<String> differentMsn =
        new SingleRecord<>(null, "recordKey", getUniqueTime(), 8467110L, getTestCells());
    SingleRecord<String> differentCells =
        new SingleRecord<>(null, "recordKey", getUniqueTime(), 8675309L, Arrays.asList(STRING_CELL, DOUBLE_CELL, BYTES_CELL));
    SingleRecord<String> differentKey =
        new SingleRecord<>(null, "differentKey", getUniqueTime(), 8675309L, getTestCells());

    //noinspection EqualsWithItself
    assertTrue(record.equals(record));

    assertTrue(record.equals(identicalRecord));
    assertTrue(identicalRecord.equals(record));
    assertEquals(record.hashCode(), identicalRecord.hashCode());

    // Each Record instance created by dataset mutation will have a different MSN -- testing
    // records with different cell content but the same MSN is testing an currently impossible case
    assertTrue(record.equals(differentCells));
    assertTrue(differentCells.equals(record));
    assertEquals(record.hashCode(), differentCells.hashCode());

    assertFalse(record.equals(differentMsn));
    assertFalse(differentMsn.equals(record));

    assertFalse(record.equals(differentKey));
    assertFalse(differentKey.equals(record));
  }

  @Test
  public void testIsEmpty() throws Exception {
    SingleRecord<String> record =
        new SingleRecord<>(null, "recordKey", getUniqueTime(), 8675309L, getTestCells());
    SingleRecord<String> emptyRecord =
        new SingleRecord<>(null, "recordKey", getUniqueTime(), 8675309L, Collections.emptySet());

    assertFalse(record.isEmpty());
    assertTrue(emptyRecord.isEmpty());
  }

  @Test
  public void testRemove() throws Exception {
    SingleRecord<String> record = getTestRecord();
    assertThrows(() -> record.remove(null), UnsupportedOperationException.class);
    assertThrows(() -> record.remove(cell("another", "cafebabe")), UnsupportedOperationException.class);
  }

  @Test
  public void testRemoveAll() throws Exception {
    SingleRecord<String> record = getTestRecord();
    assertThrows(() -> record.removeAll(null), UnsupportedOperationException.class);
    assertThrows(() -> record.removeAll(Arrays.asList(cell("another", "cafebabe"), cell("additional", 1))), UnsupportedOperationException.class);
  }

  @Test
  public void testRemoveIf() throws Exception {
    SingleRecord<String> record = getTestRecord();
    assertThrows(() -> record.removeIf(null), UnsupportedOperationException.class);
    assertThrows(() -> record.removeIf((c) -> true), UnsupportedOperationException.class);
  }

  @Test
  public void testRetainAll() throws Exception {
    SingleRecord<String> record = getTestRecord();
    assertThrows(() -> record.retainAll(null), UnsupportedOperationException.class);
    assertThrows(() -> record.retainAll(Arrays.asList(cell("another", "cafebabe"), cell("additional", 1))), UnsupportedOperationException.class);
  }

  @Test
  public void testSize() throws Exception {
    SingleRecord<String> record =
        new SingleRecord<>(null, "recordKey", getUniqueTime(), 8675309L, getTestCells());
    SingleRecord<String> emptyRecord =
        new SingleRecord<>(null, "recordKey", getUniqueTime(), 8675309L, Collections.emptySet());

    assertThat(record.size(), is(equalTo(getTestCells().size())));
    assertThat(emptyRecord.size(), is(0));
  }

  @Test
  public void testStream() throws Exception {
    List<Cell<?>> expected = new ArrayList<>(getTestCells());
    SingleRecord<String> record = getTestRecord();

    //noinspection SimplifyStreamApiCallChains
    record.stream().forEach(expected::remove);
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testToArray() throws Exception {
    List<Cell<?>> expected = new ArrayList<>(getTestCells());
    SingleRecord<String> record = getTestRecord();

    assertThat(record.toArray(), arrayContainingInAnyOrder(expected.toArray()));
  }

  @Test
  public void testToArrayT() throws Exception {
    List<Cell<?>> expected = new ArrayList<>(getTestCells());
    SingleRecord<String> record = getTestRecord();

    assertThrows(() -> record.toArray(null), NullPointerException.class);
    assertThat(record.toArray(new Cell<?>[record.size() - 1]), arrayContainingInAnyOrder(expected.toArray()));
    Cell<?>[] bigArray = new Cell<?>[record.size() + 1];
    Arrays.fill(bigArray, cell("bogus", -1));
    assertThat(record.toArray(bigArray), is(sameInstance(bigArray)));
    assertThat(bigArray.length, is(record.size() + 1));
    assertThat(bigArray[record.size()], is(nullValue()));
    assertThat(Arrays.copyOfRange(bigArray, 0, record.size()), arrayContainingInAnyOrder(expected.toArray()));
  }

  @Test
  public void testGetByName() throws Exception {
    SingleRecord<String> record = getTestRecord();

    assertThat(record.get(STRING_CELL.definition().name()), is(equalTo(Optional.of(STRING_CELL.value()))));
    assertThat(record.get("missing"), is(equalTo(Optional.empty())));

    assertThrows(() -> record.get((String)null), NullPointerException.class);
  }

  @Test
  public void testGetByCellDefinition() throws Exception {
    SingleRecord<String> record = getTestRecord();

    assertThat(record.get(STRING_CELL.definition()), is(equalTo(Optional.of(STRING_CELL.value()))));
    assertThat(record.get(CellDefinition.defineString("missing")), is(equalTo(Optional.empty())));

    assertThrows(() -> record.get((String)null), NullPointerException.class);
  }

  private SystemTimeReference getUniqueTime() {
    SystemTimeReference alteredTime;
    do {
      alteredTime = TIME_REFERENCE_GENERATOR.get();
    } while (lastTime.compareTo(alteredTime) == 0);
    lastTime = alteredTime;
    return lastTime;
  }

  /**
   * Generates a test record containing the cells returned by {@link #getTestCells()} and a unique time but
   * a common MSN value.
   * @return a new {@code SingleRecord} instance
   */
  private SingleRecord<String> getTestRecord() {
    return new SingleRecord<>(null, "recordKey", getUniqueTime(), 8675309L, getTestCells());
  }

  private Collection<Cell<?>> getTestCells() {
    return Arrays.asList(STRING_CELL, INT_CELL, LONG_CELL, DOUBLE_CELL, BYTES_CELL, BOOL_CELL);
  }

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
