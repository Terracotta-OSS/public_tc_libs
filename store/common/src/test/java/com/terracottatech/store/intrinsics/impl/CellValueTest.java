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

import static com.terracottatech.store.definition.CellDefinition.defineDouble;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test equals and hashCode implementations in {@link CellValue} classes.
 */
public class CellValueTest {

  @Test
  public void testCellValue() {
    Object cellValue = new CellValue<>(defineInt("a"), 0);
    assertEquals(cellValue, cellValue);
    assertEquals(cellValue.hashCode(), cellValue.hashCode());

    Object same = new CellValue<>(defineInt("a"), 0);
    assertEquals(cellValue, same);
    assertEquals(cellValue.hashCode(), same.hashCode());

    Object otherValue = new CellValue<>(defineInt("a"), 1);
    assertNotEquals(cellValue, otherValue);

    Object otherCellName = new CellValue<>(defineInt("b"), 0);
    assertNotEquals(cellValue, otherCellName);

    Object otherCellType = new CellValue<>(defineLong("a"), 0L);
    assertNotEquals(cellValue, otherCellType);

    Object nanValue = new CellValue<>(defineDouble("a"), Double.NaN);
    assertNotEquals(cellValue, nanValue);

    Object anotherNanValue = new CellValue<>(defineDouble("a"), Double.NaN);
    assertEquals(nanValue, anotherNanValue);
    assertEquals(nanValue.hashCode(), anotherNanValue.hashCode());
  }
}
