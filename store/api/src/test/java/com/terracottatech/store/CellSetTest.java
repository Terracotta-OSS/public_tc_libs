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

import com.terracottatech.store.definition.CellDefinition;
import org.junit.Test;

import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 * Tests for {@link CellSet}.
 */
@SuppressWarnings("ConstantConditions")
public class CellSetTest {

  private static final String COMMON_NAME = "common";

  private static final BoolCellDefinition BOOL = CellDefinition.defineBool("boolCell");
  private static final Cell<Boolean> BOOL_CELL = BOOL.newCell(true);
  private static final BoolCellDefinition BOOL_COMMON = CellDefinition.defineBool(COMMON_NAME);
  private static final Cell<Boolean> BOOL_COMMON_CELL = BOOL_COMMON.newCell(false);

  private static final CellDefinition<byte[]> BYTES = CellDefinition.define("bytesCell", Type.BYTES);
  private static final Cell<byte[]> BYTES_CELL = BYTES.newCell(new byte[]{ (byte)0xCA, (byte)0xFE, (byte)0xBA, (byte)0xBE });
  private static final CellDefinition<byte[]> BYTES_COMMON = CellDefinition.define(COMMON_NAME, Type.BYTES);
  private static final Cell<byte[]> BYTES_COMMON_CELL = BYTES_COMMON.newCell(new byte[]{ (byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF });

  private static final DoubleCellDefinition DOUBLE = CellDefinition.defineDouble("doubleCell");
  private static final Cell<Double> DOUBLE_CELL = DOUBLE.newCell(Math.PI);
  private static final DoubleCellDefinition DOUBLE_COMMON = CellDefinition.defineDouble(COMMON_NAME);
  private static final Cell<Double> DOUBLE_COMMON_CELL = DOUBLE_COMMON.newCell(Math.E);

  private static final IntCellDefinition INT = CellDefinition.defineInt("intCell");
  private static final Cell<Integer> INT_CELL = INT.newCell(42);
  private static final IntCellDefinition INT_COMMON = CellDefinition.defineInt(COMMON_NAME);
  private static final Cell<Integer> INT_COMMON_CELL = INT_COMMON.newCell(1607);

  private static final LongCellDefinition LONG = CellDefinition.defineLong("longCell");
  private static final Cell<Long> LONG_CELL = LONG.newCell(
      BigInteger.valueOf(2).pow(63).subtract(BigInteger.valueOf(25)).longValue());
  private static final LongCellDefinition LONG_COMMON = CellDefinition.defineLong(COMMON_NAME);
  private static final Cell<Long> LONG_COMMON_CELL = LONG_COMMON.newCell(313L);

  private static final StringCellDefinition STRING = CellDefinition.defineString("stringCell");
  private static final Cell<String> STRING_CELL  = STRING.newCell("cafebabe");
  private static final StringCellDefinition STRING_COMMON = CellDefinition.defineString(COMMON_NAME);
  private static final Cell<String> STRING_COMMON_CELL = STRING_COMMON.newCell("deadbeef");

  private static final List<Cell<?>> UNIQUE_CELLS = Arrays.asList(
      BOOL_CELL, BYTES_CELL, DOUBLE_CELL, INT_CELL, LONG_CELL, STRING_CELL);

  private static final List<Cell<?>> COMMON_CELLS = Arrays.asList(
      BOOL_COMMON_CELL, BYTES_COMMON_CELL, DOUBLE_COMMON_CELL, INT_COMMON_CELL, LONG_COMMON_CELL, STRING_COMMON_CELL);


  @Test
  public void testCtor1() throws Exception {
    CellSet set = new CellSet();
    assertThat(set, is(instanceOf(CellCollection.class)));
    assertThat(set, is(instanceOf(Set.class)));
    assertThat(set, is(empty()));
  }

  @Test
  public void testCtor2() throws Exception {
    CellSet set = new CellSet(Collections.emptyList());
    assertThat(set, is(instanceOf(CellCollection.class)));
    assertThat(set, is(instanceOf(Set.class)));
    assertThat(set, is(empty()));

    set = new CellSet(UNIQUE_CELLS);
    assertThat(set, is(equalTo(new HashSet<>(UNIQUE_CELLS))));

    set = new CellSet(COMMON_CELLS);
    assertThat(set, is(equalTo(Collections.singleton(COMMON_CELLS.get(COMMON_CELLS.size() - 1)))));

    List<Cell<?>> failingList = new ArrayList<>(UNIQUE_CELLS);
    failingList.add(3, null);
    assertThrows(() -> new CellSet(failingList), NullPointerException.class);
  }

  @Test
  public void testOf() throws Exception {
    CellSet set = CellSet.of();
    assertThat(set, is(instanceOf(CellCollection.class)));
    assertThat(set, is(instanceOf(Set.class)));
    assertThat(set, is(empty()));

    set = CellSet.of(toArray(UNIQUE_CELLS));
    assertThat(set, is(equalTo(new HashSet<>(UNIQUE_CELLS))));

    set = CellSet.of(toArray(COMMON_CELLS));
    assertThat(set, is(equalTo(Collections.singleton(COMMON_CELLS.get(COMMON_CELLS.size() - 1)))));

    List<Cell<?>> failingList = new ArrayList<>(UNIQUE_CELLS);
    failingList.add(3, null);
    assertThrows(() -> CellSet.of(toArray(failingList)), NullPointerException.class);
  }

  @Test
  public void testEqualsEmpty() throws Exception {
    CellSet set = new CellSet();
    assertThat(set, is(equalTo(set)));
    assertThat(set, is(equalTo(Collections.emptySet())));
    assertThat(Collections.emptySet(), is(equalTo(set)));
    assertThat(set, is(not(equalTo(null))));
  }

  @Test
  public void testEquals() throws Exception {
    Set<Cell<?>> expectedSet = new HashSet<>(UNIQUE_CELLS);
    CellSet set = new CellSet(UNIQUE_CELLS);
    assertThat(set, is(equalTo(set)));
    assertThat(set, is(equalTo(expectedSet)));
    assertThat(expectedSet, is(equalTo(set)));
    assertThat(set.hashCode(), is(equalTo(expectedSet.hashCode())));
    assertThat(set, is(not(equalTo(null))));
  }

  @Test
  public void testHashCodeEmpty() throws Exception {
    assertThat(new CellSet().hashCode(), is(equalTo(Collections.emptySet().hashCode())));
  }

  @Test
  public void testSize() throws Exception {
    assertThat(new CellSet().size(), is(0));
    assertThat(new CellSet(UNIQUE_CELLS).size(), is(UNIQUE_CELLS.size()));
    assertThat(new CellSet(COMMON_CELLS).size(), is(1));
  }

  @Test
  public void testIsEmpty() throws Exception {
    assertThat(new CellSet().isEmpty(), is(true));
    assertThat(new CellSet(UNIQUE_CELLS).isEmpty(), is(false));
  }

  @Test
  public void testContains() throws Exception {
    CellSet set = new CellSet();
    assertThat(set.contains(BYTES_CELL), is(false));

    set = new CellSet(UNIQUE_CELLS);
    assertThat(set.contains(BYTES_CELL), is(true));

    final CellSet ls = set;
    assertThrows(() -> ls.contains(null), NullPointerException.class);
  }

  @Test
  public void testIterator() throws Exception {
    CellSet set = new CellSet();
    Iterator<Cell<?>> iterator = set.iterator();
    assertThat(iterator, is(instanceOf(Iterator.class)));
    assertThat(iterator.hasNext(), is(false));

    List<Cell<?>> expected = new ArrayList<>(UNIQUE_CELLS);
    set = new CellSet(expected);
    iterator = set.iterator();
    assertThat(iterator.hasNext(), is(true));
    Cell<?> removedCell = null;
    for (int i = 0; i < expected.size(); i++) {
      if (i == 0) {
        assertThrows(iterator::remove, IllegalStateException.class);
      }

      Cell<?> cell = iterator.next();
      assertThat(cell, notNullValue());

      if (i == 3) {
        iterator.remove();
        removedCell = cell;
        assertThrows(iterator::remove, IllegalStateException.class);
      }
    }
    assertThat(iterator.hasNext(), is(false));

    assertThat(set.size(), is(expected.size() - 1));
    assertThat(set.contains(removedCell), is(false));
  }

  @Test
  public void testSpliterator() throws Exception {
    CellSet set = new CellSet();
    Spliterator<Cell<?>> spliterator = set.spliterator();
    assertThat(spliterator, is(instanceOf(Spliterator.class)));
    assertThat(spliterator.tryAdvance((c) -> { throw new AssertionError("unexpected Consumer call"); }), is(false));

    List<Cell<?>> expected = new ArrayList<>(UNIQUE_CELLS);
    set = new CellSet(expected);
    final Spliterator<Cell<?>> split = set.spliterator();
    assertTrue((split.characteristics() & (Spliterator.SORTED | Spliterator.IMMUTABLE)) == 0);
    assertTrue(split.hasCharacteristics(Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED));
    assertThrows(split::getComparator, IllegalStateException.class);

    assertThat(split.estimateSize(), is((long)expected.size()));

    assertThrows(() -> split.tryAdvance(null), NullPointerException.class);
    assertTrue(split.tryAdvance(expected::remove));

    split.forEachRemaining(expected::remove);
    assertTrue(expected.isEmpty());

    spliterator = set.spliterator();
    Spliterator<Cell<?>> split1 = spliterator.trySplit();
    if (split1 != null) {
      assertThat(spliterator.getExactSizeIfKnown() + split1.getExactSizeIfKnown(), is((long)UNIQUE_CELLS.size()));
      assertTrue((split1.characteristics() & (Spliterator.SORTED | Spliterator.IMMUTABLE)) == 0);
      assertTrue(split1.hasCharacteristics(Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED));
    }
  }

  @Test
  public void testForEach() throws Exception {
    List<Cell<?>> expected = new ArrayList<>(UNIQUE_CELLS);
    CellSet set = new CellSet(expected);

    assertThrows(() -> set.forEach(null), NullPointerException.class);

    set.forEach(expected::remove);
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testToArray() throws Exception {
    CellSet set = new CellSet(UNIQUE_CELLS);
    Object[] array = set.toArray();
    assertThat(array, is(instanceOf(Object[].class)));
    assertThat(array, arrayContainingInAnyOrder((Object[])toArray(UNIQUE_CELLS)));
  }

  @Test
  public void testToArrayWithArray() throws Exception {
    CellSet set = new CellSet(UNIQUE_CELLS);
    Cell<?>[] array = set.toArray(new Cell<?>[set.size()]);
    assertThat(array, is(instanceOf(Cell[].class)));
    assertThat(array, arrayContainingInAnyOrder(toArray(UNIQUE_CELLS)));
    assertThrows(() -> set.toArray(null), NullPointerException.class);
  }

  @Test
  public void testAdd() throws Exception {
    CellSet set = new CellSet();
    assertThat(set.add(STRING_CELL), is(true));
    assertThat(set.add(STRING_COMMON_CELL), is(true));
    assertThat(set.add(INT_COMMON_CELL), is(false));
    assertThat(set.size(), is(2));

    set = new CellSet(UNIQUE_CELLS);
    assertThat(set.add(STRING_CELL), is(false));
    assertThat(set.size(), is(UNIQUE_CELLS.size()));

    final CellSet ls = set;
    assertThrows(() -> ls.add(null), NullPointerException.class);
    assertThat(set.size(), is(UNIQUE_CELLS.size()));     // List is unaltered
  }

  @Test
  public void testSet() throws Exception {
    CellSet set = new CellSet();
    assertThat(set.set(STRING_CELL), is(nullValue()));
    assertThat(set.set(STRING_COMMON_CELL), is(nullValue()));
    assertThat(set.set(INT_COMMON_CELL), is(STRING_COMMON_CELL));
    assertThat(set.size(), is(2));

    set = new CellSet(UNIQUE_CELLS);
    assertThat(set.set(STRING_CELL), is(STRING_CELL));
    assertThat(set.size(), is(UNIQUE_CELLS.size()));

    final CellSet ls = set;
    assertThrows(() -> ls.add(null), NullPointerException.class);
    assertThat(set.size(), is(UNIQUE_CELLS.size()));     // List is unaltered
  }

  @Test
  public void testRemoveObject() throws Exception {
    CellSet set = new CellSet(UNIQUE_CELLS);
    assertThat(set.remove(DOUBLE_CELL), is(true));
    assertThat(set.size(), is(UNIQUE_CELLS.size() - 1));

    assertThat(set.remove(DOUBLE_CELL), is(false));
    assertThat(set.size(), is(UNIQUE_CELLS.size() - 1));

    set = new CellSet(COMMON_CELLS);
    assertThat(set.remove(DOUBLE_COMMON_CELL), is(false));
    Cell<?> lastCommonCell = COMMON_CELLS.get(COMMON_CELLS.size() - 1);
    assertThat(set.remove(lastCommonCell), is(true));
    assertThat(set.size(), is(0));

    assertThat(set.remove(lastCommonCell), is(false));
    assertThat(set.size(), is(0));

    final CellSet ls = set;
    assertThrows(() -> ls.remove((Object) null), NullPointerException.class);
  }

  @Test
  public void testRemoveString() throws Exception {
    CellSet set = new CellSet(UNIQUE_CELLS);
    assertThat(set.remove(DOUBLE.name()), is(Optional.of(DOUBLE_CELL)));
    assertThat(set.size(), is(UNIQUE_CELLS.size() - 1));

    assertThat(set.remove(DOUBLE.name()), is(Optional.empty()));
    assertThat(set.size(), is(UNIQUE_CELLS.size() - 1));

    set = new CellSet(COMMON_CELLS);
    assertThat(set.remove(DOUBLE_COMMON.name()), is(Optional.of(STRING_COMMON_CELL)));
    assertThat(set.size(), is(0));

    final CellSet ls = set;
    assertThrows(() -> ls.remove((String) null), NullPointerException.class);
  }

  @Test
  public void testRemoveDefinition() throws Exception {
    CellSet set = new CellSet(UNIQUE_CELLS);
    assertThat(set.remove(DOUBLE), is(Optional.of(DOUBLE_CELL)));
    assertThat(set.size(), is(UNIQUE_CELLS.size() - 1));

    assertThat(set.remove(DOUBLE), is(Optional.empty()));
    assertThat(set.size(), is(UNIQUE_CELLS.size() - 1));

    set = new CellSet(COMMON_CELLS);
    assertThat(set.remove(DOUBLE_COMMON), is(Optional.empty()));
    Cell<?> lastCommonCell = COMMON_CELLS.get(COMMON_CELLS.size() - 1);
    assertThat(set.remove(lastCommonCell.definition()), is(Optional.of(lastCommonCell)));
    assertThat(set.size(), is(0));

    assertThat(set.remove(lastCommonCell.definition()), is(Optional.empty()));
    assertThat(set.size(), is(0));

    final CellSet ls = set;
    assertThrows(() -> ls.remove((CellDefinition<?>) null), NullPointerException.class);
  }

  @Test
  public void testRemoveIf() throws Exception {
    CellSet set = new CellSet(UNIQUE_CELLS);

    assertThrows(() -> set.removeIf(null), NullPointerException.class);
    assertTrue(set.removeIf((c) -> c.definition().name().equals(INT.name())));
    assertFalse(set.removeIf((c) -> c.definition().name().equals(COMMON_NAME)));
  }

  @Test
  public void testContainsAll() throws Exception {
    List<Cell<?>> testCells = new ArrayList<>();
    testCells.addAll(UNIQUE_CELLS);
    testCells.addAll(COMMON_CELLS);
    assertThat(testCells.size(), is(UNIQUE_CELLS.size() + COMMON_CELLS.size()));

    CellSet set = new CellSet(testCells);
    assertThat(set.containsAll(UNIQUE_CELLS), is(true));
    assertThat(set.containsAll(COMMON_CELLS), is(false));
    assertThat(set.containsAll(Collections.singleton(COMMON_CELLS.get(COMMON_CELLS.size() - 1))), is(true));

    assertThrows(() -> set.containsAll(null), NullPointerException.class);

    List<Cell<?>> failingList = new ArrayList<>(UNIQUE_CELLS);
    failingList.add(2, null);
    assertThrows(() -> set.containsAll(failingList), NullPointerException.class);
  }

  @Test
  public void testAddAll() throws Exception {
    CellSet set = new CellSet();
    assertThat(set.addAll(UNIQUE_CELLS), is(true));
    assertThat(set.size(), is(UNIQUE_CELLS.size()));
    assertThat(set, is(equalTo(new HashSet<>(UNIQUE_CELLS))));

    assertThrows(() -> set.addAll(null), NullPointerException.class);
    assertThat(set.size(), is(UNIQUE_CELLS.size()));     // List is unaltered

    List<Cell<?>> failingList = new ArrayList<>(UNIQUE_CELLS);
    failingList.add(2, null);
    assertThrows(() -> set.addAll(failingList), NullPointerException.class);
    assertThat(set.size(), is(UNIQUE_CELLS.size()));     // List is unaltered
  }

  @Test
  public void testRemoveAll() throws Exception {
    List<Cell<?>> expected = new ArrayList<>(UNIQUE_CELLS);
    expected.addAll(COMMON_CELLS);
    Collections.shuffle(expected, new Random(1));

    CellSet set = new CellSet(expected);
    assertThat(set.removeAll(COMMON_CELLS), is(equalTo(expected.removeAll(COMMON_CELLS))));

    assertThrows(() -> set.retainAll(null), NullPointerException.class);

    List<Cell<?>> failingList = new ArrayList<>(UNIQUE_CELLS);
    failingList.add(3, null);
    assertThat(set.removeAll(failingList), is(true));
    assertThat(set.size(), is(0));
  }

  @Test
  public void testRetainAll() throws Exception {
    List<Cell<?>> expected = new ArrayList<>(UNIQUE_CELLS);
    expected.addAll(COMMON_CELLS);

    CellSet set = new CellSet(expected);
    assertThat(set.retainAll(COMMON_CELLS), is(true));
    assertThat(set, is(equalTo(Collections.singleton(COMMON_CELLS.get(COMMON_CELLS.size() - 1)))));

    assertThat(set.retainAll(COMMON_CELLS), is(false));

    assertThrows(() -> set.retainAll(null), NullPointerException.class);

    List<Cell<?>> failingList = new ArrayList<>(UNIQUE_CELLS);
    failingList.add(3, null);
    assertThat(set.retainAll(failingList), is(true));
    assertThat(set.size(), is(0));
  }

  @Test
  public void testClear() throws Exception {
    CellSet set = new CellSet(UNIQUE_CELLS);
    assertThat(set.size(), is(UNIQUE_CELLS.size()));
    set.clear();
    assertThat(set.size(), is(0));
  }

  @Test
  public void testGetByDefinition() throws Exception {
    CellSet set = new CellSet(UNIQUE_CELLS);
    assertThat(set.get(DOUBLE), is(equalTo(Optional.of(DOUBLE_CELL.value()))));
    assertThat(set.get(DOUBLE_COMMON), is(Optional.empty()));

    Cell<Double> altDouble = DOUBLE.newCell(Math.E);
    set.add(altDouble);
    assertThat(set.get(DOUBLE), is(equalTo(Optional.of(DOUBLE_CELL.value()))));

    assertThrows(() -> set.get((CellDefinition<?>)null), NullPointerException.class);
  }

  @Test
  public void testGetByName() throws Exception {
    CellSet set = new CellSet(UNIQUE_CELLS);
    assertThat(set.get(DOUBLE.name()), is(equalTo(Optional.of(DOUBLE_CELL.value()))));
    assertThat(set.get(DOUBLE_COMMON.name()), is(equalTo(Optional.empty())));

    Cell<Double> altDouble = DOUBLE.newCell(Math.E);
    set.add(altDouble);
    assertThat(set.get(DOUBLE.name()), is(equalTo(Optional.of(DOUBLE_CELL.value()))));

    assertThrows(() -> set.get((String)null), NullPointerException.class);
  }

  @Test
  public void testStream() throws Exception {
    List<Cell<?>> expected = new ArrayList<>(UNIQUE_CELLS);
    CellSet set = new CellSet(expected);

    Stream<Cell<?>> stream = set.stream();
    assertThat(stream, is(instanceOf(Stream.class)));

    stream.forEach(expected::remove);
    assertTrue(expected.isEmpty());
  }

  private static Cell<?>[] toArray(List<Cell<?>> list) {
    return list.toArray(new Cell<?>[list.size()]);
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