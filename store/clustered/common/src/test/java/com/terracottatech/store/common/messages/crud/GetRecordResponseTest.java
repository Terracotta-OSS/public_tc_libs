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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GetRecordResponseTest {
  @Test
  public void missingRecord() {
    GetRecordResponse<String> response = new GetRecordResponse<>();
    assertThat(response.getData(), nullValue());
  }

  @Test
  public void emptyRecord() {
    GetRecordResponse<String> response = new GetRecordResponse<>("foo", 10L, Collections.emptyList());
    assertThat(response.getData(), notNullValue());
    assertThat(response.getData().getMsn(), is(10L));
  }

  @Test
  public void recordWithCells() {
    Cell<String> cell1 = defineString("cell1").newCell("123");
    Cell<Integer> cell2 = defineInt("cell1").newCell(456);
    GetRecordResponse<String> response = new GetRecordResponse<>("foo", 10L, Arrays.asList(cell1, cell2));
    assertThat(response.getData(), notNullValue());
    List<Cell<?>> responseCells = new ArrayList<>();
    response.getData().getCells().forEach(responseCells::add);
    assertEquals(Arrays.asList(cell1, cell2), responseCells);
    assertThat(response.getData().getMsn(), is(10L));
  }
}
