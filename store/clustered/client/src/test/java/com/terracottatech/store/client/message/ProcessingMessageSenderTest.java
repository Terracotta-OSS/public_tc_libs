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

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.crud.GetRecordMessage;
import com.terracottatech.store.common.messages.crud.GetRecordResponse;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessingMessageSenderTest {
  @Test
  public void processingHappensForFuture() throws Exception {
    MessageSender underlying = mock(MessageSender.class);
    ResponseProcessor processor = mock(ResponseProcessor.class);
    ProcessingMessageSender sender = new ProcessingMessageSender(underlying, processor);

    GetRecordMessage<String> message = new GetRecordMessage<>("key", AlwaysTrue.alwaysTrue());
    GetRecordResponse<String> underlyingResponse = new GetRecordResponse<>("key", 1L, Collections.emptySet());
    CompletableFuture<DatasetEntityResponse> future = new CompletableFuture<>();
    future.complete(underlyingResponse);
    when(underlying.sendMessage(message, SendConfiguration.ONE_SERVER)).thenReturn(future);

    GetRecordResponse<String> processedResponse = new GetRecordResponse<>("key", 2L, Collections.emptySet());
    when(processor.process(underlyingResponse)).thenReturn(processedResponse);

    Future<GetRecordResponse<String>> futureResponse = sender.sendMessage(message, SendConfiguration.ONE_SERVER);
    GetRecordResponse<String> response = futureResponse.get();
    assertEquals(processedResponse, response);
  }

  @Test
  public void processingHappensForAwait() throws Exception {
    MessageSender underlying = mock(MessageSender.class);
    ResponseProcessor processor = mock(ResponseProcessor.class);
    ProcessingMessageSender sender = new ProcessingMessageSender(underlying, processor);

    GetRecordMessage<String> message = new GetRecordMessage<>("key", AlwaysTrue.alwaysTrue());
    GetRecordResponse<String> underlyingResponse = new GetRecordResponse<>("key", 1L, Collections.emptySet());
    when(underlying.sendMessageAwaitResponse(message, SendConfiguration.ONE_SERVER)).thenReturn(underlyingResponse);

    GetRecordResponse<String> processedResponse = new GetRecordResponse<>("key", 2L, Collections.emptySet());
    when(processor.process(underlyingResponse)).thenReturn(processedResponse);

    GetRecordResponse<String> response = sender.sendMessageAwaitResponse(message, SendConfiguration.ONE_SERVER);
    assertEquals(processedResponse, response);
  }
}
