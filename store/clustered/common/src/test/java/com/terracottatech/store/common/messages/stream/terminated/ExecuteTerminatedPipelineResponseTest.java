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
package com.terracottatech.store.common.messages.stream.terminated;

import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.stream.BasePipelineProcessorMessageTest;
import com.terracottatech.store.common.messages.stream.ElementValue;
import org.junit.Test;

import static java.util.OptionalDouble.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ExecuteTerminatedPipelineResponseTest extends BasePipelineProcessorMessageTest {
  @Override
  public void testEncodeDecode() throws Exception {
    ExecuteTerminatedPipelineResponse originalResponse = new ExecuteTerminatedPipelineResponse(STREAM_ID, ElementValue.ValueType.PRIMITIVE.createElementValue(42L), null);
    ExecuteTerminatedPipelineResponse decodedReponse = encodeDecode(originalResponse, DatasetEntityResponseType.EXECUTE_TERMINATED_PIPELINE_RESPONSE);

    assertThat(decodedReponse.getExplanation().isPresent(), is(false));
    assertThat(decodedReponse.getElementValue().getValue(), is(42L));
  }

  @Test
  public void testWIthExplanation() throws Exception {
    ExecuteTerminatedPipelineResponse originalResponse = new ExecuteTerminatedPipelineResponse(STREAM_ID, ElementValue.ValueType.OPTIONAL_DOUBLE.createElementValue(of(3.14)), "explanation");
    ExecuteTerminatedPipelineResponse decodedReponse = encodeDecode(originalResponse, DatasetEntityResponseType.EXECUTE_TERMINATED_PIPELINE_RESPONSE);

    assertThat(decodedReponse.getExplanation().get(), is("explanation"));
    assertThat(decodedReponse.getElementValue().getValue(), is(of(3.14)));
  }
}
