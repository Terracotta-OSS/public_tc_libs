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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test equals and hashCode implementations in {@link ToLongFunctionAdapter} classes.
 */
public class ToDoubleFunctionAdapterTest {

  @Test
  public void testToDoubleFunctionAdapter() {
    Object adapter = new ToDoubleFunctionAdapter<>(
            new CellValue.DoubleCellValue(defineDouble("a"), 0.0));
    assertEquals(adapter, adapter);
    assertEquals(adapter.hashCode(), adapter.hashCode());

    Object same = new ToDoubleFunctionAdapter<>(
            new CellValue.DoubleCellValue(defineDouble("a"), 0.0));
    assertEquals(adapter, same);
    assertEquals(adapter.hashCode(), same.hashCode());

    Object otherValue = new ToDoubleFunctionAdapter<>(
            new CellValue.DoubleCellValue(defineDouble("a"), 1.0));
    assertNotEquals(adapter, otherValue);

    Object otherCellName = new ToDoubleFunctionAdapter<>(
            new CellValue.DoubleCellValue(defineDouble("b"), 0.0));
    assertNotEquals(adapter, otherCellName);

    Object nanValue = new ToDoubleFunctionAdapter<>(
            new CellValue.DoubleCellValue(defineDouble("a"), Double.NaN));
    assertNotEquals(adapter, nanValue);

    Object anotherNanValue = new ToDoubleFunctionAdapter<>(
            new CellValue.DoubleCellValue(defineDouble("a"), Double.NaN));
    assertEquals(nanValue, anotherNanValue);
    assertEquals(nanValue.hashCode(), anotherNanValue.hashCode());
  }
}
