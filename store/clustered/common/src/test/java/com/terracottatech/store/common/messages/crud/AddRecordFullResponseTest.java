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
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.terracottatech.store.common.messages.AssertionUtils.assertAllCellsEqual;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class AddRecordFullResponseTest {
  @Test
  public void givesAddRecordType() {
    AddRecordFullResponse<String> response = new AddRecordFullResponse<>("foo", 1L, Collections.emptyList());
    Assert.assertEquals(DatasetEntityResponseType.ADD_RECORD_FULL_RESPONSE, response.getType());
  }

  @Test
  public void givesRightMsn() throws Exception {
    AddRecordFullResponse<String> response = new AddRecordFullResponse<>("foo", 1L, Collections.emptyList());
    assertThat(response.getExisting().getMsn(), is(1L));
  }

  @Test
  public void recordWithCells() {
    Cell<String> cell1 = defineString("cell1").newCell("123");
    Cell<Integer> cell2 = defineInt("cell2").newCell(456);
    AddRecordFullResponse<String> response = new AddRecordFullResponse<>("foo", 2L, Arrays.asList(cell1, cell2));
    assertAllCellsEqual(response.getExisting().getCells(), Arrays.asList(cell1, cell2));
  }
}
