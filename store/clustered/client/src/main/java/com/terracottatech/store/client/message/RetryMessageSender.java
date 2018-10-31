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

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;

import java.util.concurrent.Future;

public class RetryMessageSender implements MessageSender {
  private final MessageSender underlying;
  private final RetryMessageAssessor retryMessageAssessor;

  public RetryMessageSender(MessageSender underlying, RetryMessageAssessor retryMessageAssessor) {
    this.underlying = underlying;
    this.retryMessageAssessor = retryMessageAssessor;
  }

  @Override
  public <R extends DatasetEntityResponse> Future<R> sendMessage(DatasetEntityMessage message, SendConfiguration configuration) {
    RetryFuture<R> result = new RetryFuture<>(underlying, configuration, retryMessageAssessor, message);
    return result;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R extends DatasetEntityResponse> R sendMessageAwaitResponse(DatasetEntityMessage message, SendConfiguration configuration) {
    DatasetEntityMessage latestMessage = message;
    DatasetEntityResponse latestResponse;

    do {
      latestResponse = underlying.sendMessageAwaitResponse(latestMessage, configuration);

      latestMessage = retryMessageAssessor.getRetryMessage(latestMessage, latestResponse);
    } while (latestMessage != null);

    return (R) latestResponse;
  }
}
