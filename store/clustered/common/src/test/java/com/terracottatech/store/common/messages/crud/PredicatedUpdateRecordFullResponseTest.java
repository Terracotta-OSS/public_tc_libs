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
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.common.messages.AssertionUtils.assertAllCellsEqual;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class PredicatedUpdateRecordFullResponseTest {
  @Test
  public void getValuesPassedToConstructor() {
    PredicatedUpdateRecordFullResponse<String> response = new PredicatedUpdateRecordFullResponse<>("foo", 1L, Collections.emptyList(), 2L, Collections.emptyList());
    assertEquals(DatasetEntityResponseType.PREDICATED_UPDATE_RECORD_FULL_RESPONSE, response.getType());
    assertThat(response.getBefore().getMsn(), is(1L));
    assertThat(response.getAfter().getMsn(), is(2L));
  }

  @Test
  public void recordWithCells() {
    Cell<String> cell1 = defineString("cell1").newCell("123");
    Cell<Integer> cell2 = defineInt("cell2").newCell(456);
    Cell<String> cell3 = defineString("cell3").newCell("789");
    Cell<Integer> cell4 = defineInt("cell4").newCell(101112);

    PredicatedUpdateRecordFullResponse<String> response = new PredicatedUpdateRecordFullResponse<>("foo", 0L, Arrays.asList(cell1, cell2), 1L, Arrays.asList(cell3, cell4));

    assertAllCellsEqual(response.getBefore().getCells(), Arrays.asList(cell1, cell2));
    assertAllCellsEqual(response.getAfter().getCells(), Arrays.asList(cell3, cell4));
  }
}
