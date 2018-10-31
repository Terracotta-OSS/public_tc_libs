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
package com.terracottatech.store.common.messages.stream;

import com.terracottatech.store.common.messages.DatasetEntityResponseType;

import static com.terracottatech.store.common.messages.stream.RemoteStreamType.BATCHED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class PipelineProcessorOpenResponseTest extends BasePipelineProcessorMessageTest {
  @Override
  public void testEncodeDecode() throws Exception {
    PipelineProcessorOpenResponse response = encodeDecode(new PipelineProcessorOpenResponse(STREAM_ID, BATCHED), DatasetEntityResponseType.PIPELINE_PROCESSOR_OPEN_RESPONSE);
    assertThat(response.getSourceType(), is(BATCHED));
  }
}
