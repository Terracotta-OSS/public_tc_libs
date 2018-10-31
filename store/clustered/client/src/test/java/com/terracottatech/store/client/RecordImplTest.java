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
package com.terracottatech.store.client;

import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.StringCellDefinition;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;

import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RecordImplTest {

  private static final Cell<String> STRING_CELL = cell("stringCell", "deadbeef");
  private static final Cell<Integer> INT_CELL = cell("intCell", 1776);
  private static final Cell<Long> LONG_CELL = cell("longCell", BigInteger.valueOf(2).pow(63).subtract(BigInteger.valueOf(25)).longValue());
  private static final Cell<Double> DOUBLE_CELL = cell("doubleCell", Math.PI);
  private static final Cell<byte[]> BYTES_CELL = cell("bytesCell", new byte[] { (byte)0xCA, (byte)0xFE, (byte)0xBA, (byte)0xBE });
  private static final Cell<Boolean> BOOL_CELL = cell("boolCell", false);

  @Test
  public void hasCorrectCells() {
    StringCellDefinition cell1 = defineString("cell1");
    StringCellDefinition cell2 = defineString("cell2");
    StringCellDefinition cell3 = defineString("cell3");

    RecordImpl<String> record = new RecordImpl<>(0L, "key", Arrays.asList(cell1.newCell("abc"), cell2.newCell("def")));

    assertEquals("key", record.getKey());
    assertEquals("abc", record.get(cell1).get());
    assertEquals("def", record.get(cell2).get());
    assertFalse(record.get(cell3).isPresent());

    assertEquals("abc", record.get("cell1").get());
    assertEquals("def", record.get("cell2").get());
    assertFalse(record.get("cell3").isPresent());

    for (Cell<?> cell : record) {
      if (cell.definition() == cell1) {
        assertEquals("abc", cell.value());
      } else if (cell.definition() == cell2) {
        assertEquals("def", cell.value());
      } else {
        fail("Unexpected cell");
      }
    }
  }

  @Test
  public void testAdd() throws Exception {
    RecordImpl<String> record = getRecord(0L, "recordKey");
    assertThrows(() -> record.add(null), UnsupportedOperationException.class);
    assertThrows(() -> record.add(STRING_CELL), UnsupportedOperationException.class);
  }

  @Test
  public void testAddAll() throws Exception {
    RecordImpl<String> record = getRecord(0L, "recordKey");
    assertThrows(() -> record.addAll(null), UnsupportedOperationException.class);
    assertThrows(() -> record.addAll(Arrays.asList(STRING_CELL, LONG_CELL)), UnsupportedOperationException.class);
  }

  @Test
  public void testClear() throws Exception {
    RecordImpl<String> record = getRecord(0L, "recordKey");
    assertThrows(record::clear, UnsupportedOperationException.class);
  }

  @Test
  public void testContains() throws Exception {
    RecordImpl<String> record = getRecord(0L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL);
    assertThrows(() -> record.contains(null), NullPointerException.class);
    assertTrue(record.contains(INT_CELL));
    assertFalse(record.contains(INT_CELL.definition().newCell(5551212)));
  }

  @Test
  public void testContainsAll() throws Exception {
    RecordImpl<String> record = getRecord(0L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL);
    assertThrows(() -> record.containsAll(null), NullPointerException.class);
    assertTrue(record.containsAll(Arrays.asList(BYTES_CELL, INT_CELL)));
    assertFalse(record.containsAll(Arrays.asList(INT_CELL, INT_CELL.definition().newCell(5551212))));

    assertThrows(() -> record.containsAll(Collections.singleton(null)), NullPointerException.class);
  }

  @SuppressWarnings("EqualsWithItself")
  @Test
  public void testEquals() throws Exception {
    RecordImpl<String> record = getRecord(0L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL, STRING_CELL, LONG_CELL, BOOL_CELL);
    RecordImpl<String> identicalRecord = getRecord(0L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL, STRING_CELL, LONG_CELL, BOOL_CELL);
    RecordImpl<String> reorderedRecord = getRecord(0L, "recordKey", DOUBLE_CELL, BOOL_CELL, STRING_CELL, BYTES_CELL, INT_CELL, LONG_CELL);
    RecordImpl<String> differentCellsSameMsn = getRecord(0L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL.definition().newCell(Math.E), STRING_CELL, LONG_CELL, BOOL_CELL);
    RecordImpl<String> noCellsSameMsn = getRecord(0L, "recordKey");
    RecordImpl<String> identicalRecordDifferentMsn = getRecord(1L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL, STRING_CELL, LONG_CELL, BOOL_CELL);
    RecordImpl<String> differentCellsDifferentMsn = getRecord(1L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL.definition().newCell(Math.E), STRING_CELL, LONG_CELL, BOOL_CELL);
    RecordImpl<String> noCellsDifferentMsn = getRecord(1L, "recordKey");

    assertTrue(record.equals(record));
    assertTrue(noCellsSameMsn.equals(noCellsSameMsn));

    assertTrue(record.equals(identicalRecord));
    assertTrue(identicalRecord.equals(record));
    assertEquals(record.hashCode(), identicalRecord.hashCode());

    assertTrue(record.equals(reorderedRecord));
    assertTrue(reorderedRecord.equals(record));
    assertEquals(record.hashCode(), reorderedRecord.hashCode());

    assertFalse(record.equals(differentCellsSameMsn));
    assertFalse(differentCellsSameMsn.equals(record));

    assertFalse(record.equals(noCellsSameMsn));
    assertFalse(noCellsSameMsn.equals(record));

    assertTrue(record.equals(identicalRecordDifferentMsn));
    assertTrue(identicalRecordDifferentMsn.equals(record));

    assertFalse(record.equals(differentCellsDifferentMsn));
    assertFalse(differentCellsDifferentMsn.equals(record));

    assertFalse(record.equals(noCellsDifferentMsn));
    assertFalse(noCellsDifferentMsn.equals(record));
  }

  @Test
  public void testGetEqualsPredicate() throws Exception {
    RecordImpl<String> record = getRecord(0L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL, STRING_CELL, LONG_CELL, BOOL_CELL);
    RecordImpl<String> identicalRecord = getRecord(0L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL, STRING_CELL, LONG_CELL, BOOL_CELL);
    RecordImpl<String> reorderedRecord = getRecord(0L, "recordKey", DOUBLE_CELL, BOOL_CELL, STRING_CELL, BYTES_CELL, INT_CELL, LONG_CELL);
    RecordImpl<String> differentCellsSameMsn = getRecord(0L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL.definition().newCell(Math.E), STRING_CELL, LONG_CELL, BOOL_CELL);
    RecordImpl<String> noCellsSameMsn = getRecord(0L, "recordKey");
    RecordImpl<String> identicalRecordDifferentMsn = getRecord(1L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL, STRING_CELL, LONG_CELL, BOOL_CELL);
    RecordImpl<String> differentCellsDifferentMsn = getRecord(1L, "recordKey", BYTES_CELL, INT_CELL, DOUBLE_CELL.definition().newCell(Math.E), STRING_CELL, LONG_CELL, BOOL_CELL);
    RecordImpl<String> noCellsDifferentMsn = getRecord(1L, "recordKey");

    assertTrue(record.getEqualsPredicate().test(record));

    assertTrue(record.getEqualsPredicate().test(identicalRecord));
    assertTrue(identicalRecord.getEqualsPredicate().test(record));
    assertEquals(record.hashCode(), identicalRecord.hashCode());

    assertTrue(record.getEqualsPredicate().test(reorderedRecord));
    assertTrue(reorderedRecord.getEqualsPredicate().test(record));
    assertEquals(record.hashCode(), reorderedRecord.hashCode());

    assertFalse(record.getEqualsPredicate().test(differentCellsSameMsn));
    assertFalse(differentCellsSameMsn.getEqualsPredicate().test(record));

    assertFalse(record.getEqualsPredicate().test(noCellsSameMsn));
    assertFalse(noCellsSameMsn.getEqualsPredicate().test(record));

    assertTrue(record.getEqualsPredicate().test(identicalRecordDifferentMsn));
    assertTrue(identicalRecordDifferentMsn.getEqualsPredicate().test(record));

    assertFalse(record.getEqualsPredicate().test(differentCellsDifferentMsn));
    assertFalse(differentCellsDifferentMsn.getEqualsPredicate().test(record));

    assertFalse(record.getEqualsPredicate().test(noCellsDifferentMsn));
    assertFalse(noCellsDifferentMsn.getEqualsPredicate().test(record));
  }

  @Test
  public void testIsEmpty() throws Exception {
    assertTrue(getRecord(0L, "recordKey").isEmpty());
    assertFalse((getRecord(0L, "recordKey", DOUBLE_CELL).isEmpty()));
  }

  @Test
  public void testIterator() throws Exception {
    Cell<?>[] cells = { BYTES_CELL, INT_CELL, DOUBLE_CELL };
    RecordImpl<String> record = getRecord(0L, "recordKey", cells);
    Iterator<Cell<?>> iterator = record.iterator();
    assertThat(iterator, is(notNullValue()));

    assertTrue(iterator.hasNext());

    List<Cell<?>> expectedCells = new ArrayList<>(Arrays.asList(cells));
    assertTrue(expectedCells.remove(iterator.next()));
    assertThrows(iterator::remove, UnsupportedOperationException.class);

    iterator.forEachRemaining(expectedCells::remove);
    assertTrue(expectedCells.isEmpty());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testRemove() throws Exception {
    RecordImpl<String> record = getRecord(0L, "recordKey");
    assertThrows(() -> record.remove(null), UnsupportedOperationException.class);
    assertThrows(() -> record.remove(STRING_CELL), UnsupportedOperationException.class);
  }

  @Test
  public void testRemoveAll() throws Exception {
    RecordImpl<String> record = getRecord(0L, "recordKey");
    assertThrows(() -> record.removeAll(null), UnsupportedOperationException.class);
    assertThrows(() -> record.removeAll(Arrays.asList(STRING_CELL, LONG_CELL)), UnsupportedOperationException.class);
  }

  @Test
  public void testRemoveIf() throws Exception {
    RecordImpl<String> record = getRecord(0L, "recordKey");
    assertThrows(() -> record.removeIf(null), UnsupportedOperationException.class);
    assertThrows(() -> record.removeIf((c) -> true), UnsupportedOperationException.class);
  }

  @Test
  public void testRetainAll() throws Exception {
    RecordImpl<String> record = getRecord(0L, "recordKey");
    assertThrows(() -> record.retainAll(null), UnsupportedOperationException.class);
    assertThrows(() -> record.retainAll(Arrays.asList(STRING_CELL, LONG_CELL)), UnsupportedOperationException.class);
  }

  @Test
  public void testSize() throws Exception {
    assertThat(getRecord(0L, "recordKey").size(), is(0));
    assertThat(getRecord(0L, "recordKey", STRING_CELL, DOUBLE_CELL, BYTES_CELL).size(), is(3));
  }

  @Test
  public void testSpliterator() throws Exception {
    Cell<?>[] cells = { STRING_CELL, BOOL_CELL, BYTES_CELL, INT_CELL, DOUBLE_CELL, LONG_CELL };
    Collection<Cell<?>> expectedCells = new ArrayList<>(Arrays.asList(cells));
    RecordImpl<String> record = getRecord(0L, "recordKey", cells);

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
  public void testStream() throws Exception {
    Cell<?>[] cells = { STRING_CELL, BOOL_CELL, BYTES_CELL, INT_CELL, DOUBLE_CELL, LONG_CELL };
    Collection<Cell<?>> expectedCells = new ArrayList<>(Arrays.asList(cells));
    RecordImpl<String> record = getRecord(0L, "recordKey", cells);

    //noinspection SimplifyStreamApiCallChains
    record.stream().forEach(expectedCells::remove);
    assertTrue(expectedCells.isEmpty());
  }

  @Test
  public void testToArray() throws Exception {
    Cell<?>[] cells = { STRING_CELL, BOOL_CELL, BYTES_CELL, INT_CELL, DOUBLE_CELL, LONG_CELL };
    RecordImpl<String> record = getRecord(0L, "recordKey", cells);

    assertThat(record.toArray(), arrayContainingInAnyOrder((Object[])cells));
  }

  @Test
  public void testToArrayT() throws Exception {
    Cell<?>[] cells = { STRING_CELL, BOOL_CELL, BYTES_CELL, INT_CELL, DOUBLE_CELL, LONG_CELL };
    RecordImpl<String> record = getRecord(0L, "recordKey", cells);

    assertThrows(() -> record.toArray(null), NullPointerException.class);
    assertThat(record.toArray(new Cell<?>[record.size() - 1]), arrayContainingInAnyOrder(cells));
    Cell<?>[] bigArray = new Cell<?>[record.size() + 1];
    Arrays.fill(bigArray, cell("bogus", -1));
    assertThat(record.toArray(bigArray), is(sameInstance(bigArray)));
    assertThat(bigArray.length, is(record.size() + 1));
    assertThat(bigArray[record.size()], is(nullValue()));
    assertThat(Arrays.copyOfRange(bigArray, 0, record.size()), arrayContainingInAnyOrder(cells));
  }

  @Test
  public void testForEach() throws Exception {
    Cell<?>[] cells = { STRING_CELL, BOOL_CELL, BYTES_CELL, INT_CELL, DOUBLE_CELL, LONG_CELL };
    Collection<Cell<?>> expectedCells = new ArrayList<>(Arrays.asList(cells));
    RecordImpl<String> record = getRecord(0L, "recordKey", cells);

    record.forEach(expectedCells::remove);
    assertTrue(expectedCells.isEmpty());
  }

  @Test(expected = NullPointerException.class)
  public void testCtorWithNullMsn() throws Exception {
    new RecordImpl<>(null, "bob", Collections.emptyList());
  }

  @Test(expected = NullPointerException.class)
  public void testCtorWithNullKey() throws Exception {
    new RecordImpl<>(1L, null, Collections.emptyList());
  }

  @Test(expected = NullPointerException.class)
  public void testCtorWithNullCells() throws Exception {
    new RecordImpl<>(1L, "bob", null);
  }

  private <K extends Comparable<K>> RecordImpl<K> getRecord(Long msn, K key, Cell<?>... cells) {
    return new RecordImpl<>(msn, key, Arrays.asList(cells));
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
