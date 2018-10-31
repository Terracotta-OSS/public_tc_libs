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

import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.stream.BasePipelineProcessorMessageTest;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;

import java.util.List;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.FILTER;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.DELETE;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class ExecuteTerminatedPipelineMessageTest extends BasePipelineProcessorMessageTest {
  @Override
  public void testEncodeDecode() throws Exception {
    List<PipelineOperation> operations = singletonList(FILTER.newInstance(AlwaysTrue.alwaysTrue()));
    PipelineOperation terminal = DELETE.newInstance();
    ExecuteTerminatedPipelineMessage originalMessage = new ExecuteTerminatedPipelineMessage(STREAM_ID, operations, terminal, true, true);
    assertThat(originalMessage.getType(), is(DatasetOperationMessageType.EXECUTE_TERMINATED_PIPELINE_MESSAGE));

    ExecuteTerminatedPipelineMessage decodedMessage = encodeDecode(originalMessage);

    assertEqualOps(decodedMessage.getIntermediateOperations(), operations);
    assertEqualOperation(decodedMessage.getTerminalOperation(), terminal);
    assertThat(decodedMessage.getRequiresExplanation(), is(true));
    assertThat(decodedMessage.isMutative(), is(true));
  }
}
