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
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;

import java.util.List;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.FILTER;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.MUTATE;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class PipelineProcessorOpenMessageTest extends BasePipelineProcessorMessageTest {
  @Override
  public void testEncodeDecode() throws Exception {
    List<PipelineOperation> operations = singletonList(FILTER.newInstance(AlwaysTrue.alwaysTrue()));
    PipelineOperation terminal = MUTATE.newInstance(new NonPortableTransform<>(-1));
    PipelineProcessorOpenMessage originalMessage = new PipelineProcessorOpenMessage(STREAM_ID, RemoteStreamType.INLINE, ElementType.RECORD, operations, terminal, true);
    PipelineProcessorOpenMessage decodedMessage = encodeDecode(originalMessage, DatasetOperationMessageType.PIPELINE_PROCESSOR_OPEN_MESSAGE);

    assertEqualOps(decodedMessage.getPortableOperations(), operations);
    assertEqualOperation(decodedMessage.getTerminalOperation(), terminal);
    assertThat(decodedMessage.getRequiresExplanation(), is(true));
  }
}

