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
package com.terracottatech.store.common.messages.stream.inline;

import com.terracottatech.store.Cell;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.RecordData;
import com.terracottatech.store.common.messages.stream.BasePipelineProcessorMessageTest;
import com.terracottatech.store.common.messages.stream.Element;
import com.terracottatech.store.common.messages.stream.ElementType;
import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class TryAdvanceFetchApplyResponseTest extends BasePipelineProcessorMessageTest {

  @Override
  public void testEncodeDecode() throws Exception {
    Collection<Cell<?>> cells = createCells();
    TryAdvanceFetchApplyResponse<String> originalResponse = new TryAdvanceFetchApplyResponse<>(STREAM_ID, new RecordData<>(Long.MAX_VALUE, "recordKey", cells));
    TryAdvanceFetchApplyResponse<String> decodedResponse = encodeDecode(originalResponse, DatasetEntityResponseType.TRY_ADVANCE_FETCH_APPLY_RESPONSE);

    Element element = decodedResponse.getElement();
    assertThat(element.getType(), is(ElementType.RECORD));

    RecordData<String> recordData = element.getRecordData();
    assertThat(recordData, is(notNullValue()));
    assertThat(recordData.getMsn(), is(Long.MAX_VALUE));
    assertThat(recordData.getKey(), is("recordKey"));
    assertThat(recordData.getCells(), containsInAnyOrder(cells.toArray()));

    assertThrows(element::getDoubleValue, ClassCastException.class);
    assertThrows(element::getIntValue, ClassCastException.class);
    assertThrows(element::getLongValue, ClassCastException.class);
  }

  @Test
  public void testTryAdvanceFetchApplyResponseNull() throws Exception {
    TryAdvanceFetchApplyResponse<String> originalResponse = new TryAdvanceFetchApplyResponse<>(STREAM_ID, (Element)null);
    TryAdvanceFetchApplyResponse<String> decodedResponse = encodeDecode(originalResponse, DatasetEntityResponseType.TRY_ADVANCE_FETCH_APPLY_RESPONSE);

    Element element = decodedResponse.getElement();
    assertThat(element, is(nullValue()));
  }

}
