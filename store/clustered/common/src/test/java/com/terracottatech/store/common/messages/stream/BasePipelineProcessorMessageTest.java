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

import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.messages.BaseMessageTest;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.intrinsics.Intrinsic;
import org.terracotta.entity.MessageCodecException;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;

public abstract class BasePipelineProcessorMessageTest extends BaseMessageTest {

  protected <M extends PipelineProcessorMessage> M encodeDecode(M originalMessage, DatasetOperationMessageType expectedType)
          throws MessageCodecException {
    assertThat(originalMessage.getType(), is(expectedType));

    M decodedMessage = encodeDecode(originalMessage);

    assertThat(decodedMessage.getType(), is(originalMessage.getType()));
    assertThat(decodedMessage, is(instanceOf(originalMessage.getClass())));
    assertThat(decodedMessage.getStreamId(), is(originalMessage.getStreamId()));
    return decodedMessage;
  }

  @SuppressWarnings("unchecked")
  protected <R extends PipelineProcessorResponse> R encodeDecode(R originalResponse, DatasetEntityResponseType expectedType)
          throws MessageCodecException {
    assertThat(originalResponse.getType(), is(expectedType));

    R decodedResponse = encodeDecode(originalResponse);

    assertThat(decodedResponse.getType(), is(originalResponse.getType()));
    assertThat(decodedResponse, is(instanceOf(originalResponse.getClass())));
    assertThat(decodedResponse.getStreamId(), is(originalResponse.getStreamId()));
    return decodedResponse;
  }

  protected void assertEqualOps(List<PipelineOperation> actualOps, List<PipelineOperation> expectedOps) {
    assertEquals(actualOps.size(), expectedOps.size());

    for (int i = 0; i < actualOps.size(); i++) {
      assertEqualOperation(actualOps.get(i), expectedOps.get(i));
    }
  }

  protected void assertEqualOperation(PipelineOperation actualOp, PipelineOperation expectedOp) {
    assertEquals(actualOp.getOperation(), expectedOp.getOperation());
    assertEquals(actualOp.getOperationMetaData(), expectedOp.getOperationMetaData());

    List<Object> actualOpArgs = actualOp.getArguments();
    List<Object> expectedOpArgs = expectedOp.getArguments();
    assertEquals(actualOpArgs.size(), expectedOpArgs.size());
    for (int j = 0; j < actualOpArgs.size(); j++) {
      Intrinsic actualOpArg = (Intrinsic)actualOpArgs.get(j);
      Intrinsic expectedOpArg = (Intrinsic)expectedOpArgs.get(j);
      assertEquals(actualOpArg.getClass(), expectedOpArg.getClass());
      assertEquals(actualOpArg.getIntrinsicType(), expectedOpArg.getIntrinsicType());
    }
  }
}
