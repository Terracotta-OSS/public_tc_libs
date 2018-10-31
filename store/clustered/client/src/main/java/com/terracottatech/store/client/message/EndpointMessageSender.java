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

import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.client.InvokeFutureFuture;
import com.terracottatech.store.common.InterruptHelper;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.InvocationBuilder;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.MessageCodecException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class EndpointMessageSender implements MessageSender {
  private final EntityClientEndpoint<DatasetEntityMessage, DatasetEntityResponse> endpoint;

  public EndpointMessageSender(EntityClientEndpoint<DatasetEntityMessage, DatasetEntityResponse> endpoint) {
    this.endpoint = endpoint;
  }

  public <R extends DatasetEntityResponse> Future<R> sendMessage(DatasetEntityMessage message, SendConfiguration configuration) {
    InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> invocationBuilder = endpoint.beginInvoke();
    configuration.configure(invocationBuilder);
    invocationBuilder.message(message);

    try {
      InvokeFuture<DatasetEntityResponse> invokeFuture = invocationBuilder.invoke();
      InvokeFutureFuture<DatasetEntityResponse> invokeFutureFuture = new InvokeFutureFuture<>(invokeFuture);
      return new ResponseProcessingFuture<>(invokeFutureFuture, new CastingResponseProcessor());
    } catch (MessageCodecException e) {
      throw new StoreRuntimeException(e);
    }
  }

  public <R extends DatasetEntityResponse> R sendMessageAwaitResponse(DatasetEntityMessage message, SendConfiguration configuration) {
    Future<R> future = sendMessage(message, configuration);

    try {
      return InterruptHelper.getUninterruptibly(future);
    } catch (ExecutionException e) {
      throw new StoreRuntimeException(e.getCause());
    }
  }
}
