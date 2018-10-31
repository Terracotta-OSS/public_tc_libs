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

package com.terracottatech.store.common.messages.crud;

import com.terracottatech.store.Cell;
import com.terracottatech.store.ChangeType;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.common.messages.AssertionUtils.assertAllCellsEqual;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AddRecordMessageTest {
  private final UUID stableClientId = UUID.randomUUID();

  @Test
  public void noCells() {
    AddRecordMessage<String> message =
        new AddRecordMessage<>(stableClientId, "key", Collections.emptyList(), true);
    assertEquals(DatasetOperationMessageType.ADD_RECORD_MESSAGE, message.getType());
    assertEquals(ChangeType.ADDITION, message.getChangeType());
    assertEquals(stableClientId, message.getStableClientId());
    assertEquals("key", message.getKey());
    assertFalse(message.getCells().iterator().hasNext());
    assertEquals(true, message.isRespondInFull());
  }

  @Test
  public void oneCell() {
    Cell<String> cell = defineString("name").newCell("value");
    AddRecordMessage<String> message =
        new AddRecordMessage<>(stableClientId, "key", Collections.singletonList(cell), true);
    assertAllCellsEqual(Collections.singletonList(cell), message.getCells());
  }

  @Test
  public void multipleCells() {
    Cell<String> cell1 = defineString("name1").newCell("value1");
    Cell<String> cell2 = defineString("name2").newCell("value2");
    AddRecordMessage<String> message =
        new AddRecordMessage<>(stableClientId, "key", Arrays.asList(cell1, cell2), true);
    assertAllCellsEqual(Arrays.asList(cell1, cell2), message.getCells());
  }
}
