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

import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Tests default methods in {@link CellCollection}.
 */
public class CellCollectionTest {

  private static final StringCellDefinition STRING_CELL = CellDefinition.defineString("string");
  private static final IntCellDefinition INT_CELL = CellDefinition.defineInt("int");
  private static final LongCellDefinition LONG_CELL = CellDefinition.defineLong("long");
  private static final DoubleCellDefinition DOUBLE_CELL = CellDefinition.defineDouble("double");
  private static final BoolCellDefinition BOOLEAN_CELL = CellDefinition.defineBool("boolean");
  private static final CharCellDefinition CHAR_CELL = CellDefinition.defineChar("char");
  private static final CellDefinition<byte[]> BYTES_CELL = CellDefinition.define("bytes", Type.BYTES);
  private static final long LONG_VALUE = 8675309L;

  @Test
  public void testGetByName() throws Exception {
    CellCollection record = getTestRecord();
    assertThat(record.get(LONG_CELL.name()), is(equalTo(Optional.of(LONG_VALUE))));
    assertThat(record.get("missingCell"), is(equalTo(Optional.empty())));
  }

  @Test
  public void testGetByNameNull() throws Exception {
    CellCollection record = getTestRecord();
    try {
      record.get((String)null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testGetByCellDefinition() throws Exception {
    CellCollection record = getTestRecord();
    assertThat(record.get(LONG_CELL), is(equalTo(Optional.of(LONG_VALUE))));
    assertThat(record.get(CellDefinition.defineString("missing")), is(equalTo(Optional.empty())));
  }

  @Test
  public void testGetByCellDefinitionNull() throws Exception {
    CellCollection record = getTestRecord();
    try {
      record.get((CellDefinition<?>)null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  private CellCollection getTestRecord() {
    return new TestCellCollection(
        INT_CELL.newCell(7),
        DOUBLE_CELL.newCell(Math.PI),
        BOOLEAN_CELL.newCell(Boolean.FALSE),
        BYTES_CELL.newCell(new byte[] { (byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF }),
        LONG_CELL.newCell(LONG_VALUE),
        STRING_CELL.newCell("stringValue"),
        CHAR_CELL.newCell('\u2422')
    );
  }

  private static final class TestCellCollection implements CellCollection {

    private final Map<String, Cell<?>> cells;

    TestCellCollection(Cell<?>... cells) {
      Map<String, Cell<?>> cellMap;
      if (cells.length > 0) {
        cellMap = new LinkedHashMap<>(cells.length);
        for (Cell<?> cell : cells) {
          cellMap.put(cell.definition().name(), cell);
        }
        cellMap = Collections.unmodifiableMap(cellMap);
      } else {
        cellMap = Collections.emptyMap();
      }
      this.cells = cellMap;
    }

    @Override
    public int size() {
      return cells.size();
    }

    @Override
    public boolean isEmpty() {
      return cells.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Cell<?>> iterator() {
      return this.cells.values().iterator();
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Cell<?> cell) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Cell<?>> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }
}