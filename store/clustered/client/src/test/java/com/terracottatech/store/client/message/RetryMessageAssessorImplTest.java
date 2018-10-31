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
package com.terracottatech.store.client.message;

import com.terracottatech.store.common.messages.BusyResponse;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorRequestResultMessage;
import com.terracottatech.store.common.messages.stream.PipelineRequestProcessingResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;

import static com.terracottatech.store.common.messages.BusyResponse.Reason.KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RetryMessageAssessorImplTest {
  @Mock
  private DatasetEntityMessage message;

  @Mock
  private DatasetEntityResponse response;

  @Mock
  private PipelineRequestProcessingResponse pipelineResponse;

  @Test
  public void noRetry() {
    RetryMessageAssessor assessor = new RetryMessageAssessorImpl();
    DatasetEntityMessage retryMessage = assessor.getRetryMessage(message, response);

    assertNull(retryMessage);
  }

  @Test
  public void retryBusy() {
    RetryMessageAssessor assessor = new RetryMessageAssessorImpl();
    BusyResponse busyResponse = new BusyResponse(KEY);
    DatasetEntityMessage retryMessage = assessor.getRetryMessage(message, busyResponse);

    assertEquals(message, retryMessage);
  }

  @Test
  public void retryPipeline() {
    UUID uuid = UUID.randomUUID();
    when(pipelineResponse.getStreamId()).thenReturn(uuid);

    RetryMessageAssessor assessor = new RetryMessageAssessorImpl();
    DatasetEntityMessage retryMessage = assessor.getRetryMessage(message, pipelineResponse);

    PipelineProcessorRequestResultMessage pipelineRetryMessage = (PipelineProcessorRequestResultMessage) retryMessage;
    assertEquals(uuid, pipelineRetryMessage.getStreamId());
  }
}
