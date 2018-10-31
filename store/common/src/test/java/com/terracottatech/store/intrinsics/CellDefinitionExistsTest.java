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
package com.terracottatech.store.intrinsics;

import com.terracottatech.store.intrinsics.impl.RecordSameness;
import org.junit.Test;

import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test equals and hashCode implementations in {@link CellDefinitionExists} classes.
 */
public class CellDefinitionExistsTest {

  @Test
  public void testCellDefinitionExists() {
    CellDefinitionExists exists = new CellDefinitionExists(defineInt("a"));
    assertEquals(exists, exists);
    assertEquals(exists.hashCode(), exists.hashCode());

    Intrinsic same = new CellDefinitionExists(defineInt("a"));
    assertEquals(exists, same);
    assertEquals(exists.hashCode(), same.hashCode());

    Intrinsic otherCellName = new CellDefinitionExists(defineInt("b"));
    assertNotEquals(exists, otherCellName);

    Intrinsic otherCellType = new CellDefinitionExists(defineLong("a"));
    assertNotEquals(exists, otherCellType);

    Intrinsic other = new RecordSameness<>(0, "a");
    assertNotEquals(exists, other);
  }
}
