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

import java.util.UUID;

public class RetryMessageAssessorImpl implements RetryMessageAssessor {
  @Override
  public DatasetEntityMessage getRetryMessage(DatasetEntityMessage latestMessage, DatasetEntityResponse latestResponse) {
    if (latestResponse instanceof BusyResponse) {
      // We did consider having an exponential back-off delay here, but it could lead to unfairness / starvation and
      // clients that have backed-off are less likely to succeed than "fresh" clients. This leads to the population of
      // clients moving towards longer delays on average, rather than prioritising clients that having been waiting a long
      // time. Also, adding a delay means that those operations that retry would have some extra latency. The presumed
      // benefit of a delay is to reduce the load on the server, but the server will provide a natural back-pressure by
      // taking some length of time to process each message - the more loaded the server is, the longer it will take.

      return latestMessage;
    }

    if (latestResponse instanceof PipelineRequestProcessingResponse) {
      PipelineRequestProcessingResponse pipelineResponse = (PipelineRequestProcessingResponse) latestResponse;
      UUID streamId = pipelineResponse.getStreamId();

      return new PipelineProcessorRequestResultMessage(streamId);
    }

    // No need to retry - this response is good.
    return null;
  }
}
