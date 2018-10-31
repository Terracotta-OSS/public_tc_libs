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
package com.terracottatech.store.common.messages.stream.batched;

import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.stream.BasePipelineProcessorMessageTest;
import com.terracottatech.store.common.messages.stream.Element;
import com.terracottatech.store.common.messages.stream.ElementType;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BatchFetchResponseTest extends BasePipelineProcessorMessageTest {

  @Override
  public void testEncodeDecode() throws Exception {
    Random rndm = new Random();
    BatchFetchResponse originalMessage = new BatchFetchResponse(STREAM_ID, range(0, 10).mapToObj(i -> new Element(ElementType.INT, i)).collect(toList()));
    BatchFetchResponse decodedMessage = encodeDecode(originalMessage, DatasetEntityResponseType.BATCH_FETCH_RESPONSE);

    List<Element> elements = decodedMessage.getElements();
    for (int i = 0; i < 10; i++) {
      assertThat(elements.get(i).getType(), is(ElementType.INT));
      assertThat(elements.get(i).getIntValue(), is(i));
    }
  }

  @Test
  public void testEmptyBatch() throws Exception {
    Random rndm = new Random();
    UUID randomUuid = new UUID(rndm.nextLong(), rndm.nextLong());
    BatchFetchResponse originalMessage = new BatchFetchResponse(randomUuid, emptyList());
    BatchFetchResponse decodedMessage = encodeDecode(originalMessage, DatasetEntityResponseType.BATCH_FETCH_RESPONSE);

    assertThat(decodedMessage.getStreamId(), is(randomUuid));
    assertThat(decodedMessage.getElements(), empty());
  }
}
