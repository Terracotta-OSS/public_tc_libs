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

import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test equals and hashCode implementations in {@link ToIntFunctionAdapter} classes.
 */
public class ToIntFunctionAdapterTest {

  @Test
  public void testToIntFunctionAdapter() {
    Object adapter = new ToIntFunctionAdapter<>(
            new CellValue.IntCellValue(defineInt("a"), 0));
    assertEquals(adapter, adapter);
    assertEquals(adapter.hashCode(), adapter.hashCode());

    Object same = new ToIntFunctionAdapter<>(
            new CellValue.IntCellValue(defineInt("a"), 0));
    assertEquals(adapter, same);
    assertEquals(adapter.hashCode(), same.hashCode());

    Object otherValue = new ToIntFunctionAdapter<>(
            new CellValue.IntCellValue(defineInt("a"), 1));
    assertNotEquals(adapter, otherValue);

    Object otherCellName = new ToIntFunctionAdapter<>(
            new CellValue.IntCellValue(defineInt("b"), 0));
    assertNotEquals(adapter, otherCellName);
  }
}
