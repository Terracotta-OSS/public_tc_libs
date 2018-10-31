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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RetryMessageSenderTest {
  @Mock
  private MessageSender underlying;

  @Mock
  private RetryMessageAssessor assessor;

  @Mock
  private DatasetEntityMessage message1;

  @Mock
  private DatasetEntityMessage message2;

  @Mock
  private DatasetEntityResponse response1;

  @Mock
  private DatasetEntityResponse response2;

  @Test
  public void noRetryAwaitResponse() {
    when(underlying.sendMessageAwaitResponse(message1, SendConfiguration.FULL)).thenReturn(response1);
    when(assessor.getRetryMessage(message1, response1)).thenReturn(null);

    RetryMessageSender sender = new RetryMessageSender(underlying, assessor);
    DatasetEntityResponse finalResponse = sender.sendMessageAwaitResponse(message1, SendConfiguration.FULL);

    assertEquals(response1, finalResponse);
  }

  @Test
  public void retryAwaitResponse() {
    when(underlying.sendMessageAwaitResponse(message1, SendConfiguration.FULL)).thenReturn(response1);
    when(underlying.sendMessageAwaitResponse(message2, SendConfiguration.FULL)).thenReturn(response2);
    when(assessor.getRetryMessage(message1, response1)).thenReturn(message2);
    when(assessor.getRetryMessage(message2, response2)).thenReturn(null);

    RetryMessageSender sender = new RetryMessageSender(underlying, assessor);
    DatasetEntityResponse finalResponse = sender.sendMessageAwaitResponse(message1, SendConfiguration.FULL);

    assertEquals(response2, finalResponse);
  }

  @Test
  public void noRetryNoAwait() throws Exception {
    when(underlying.sendMessage(message1, SendConfiguration.FULL)).thenReturn(CompletableFuture.completedFuture(response1));
    when(assessor.getRetryMessage(message1, response1)).thenReturn(null);

    RetryMessageSender sender = new RetryMessageSender(underlying, assessor);
    Future<DatasetEntityResponse> finalResponseFuture = sender.sendMessage(message1, SendConfiguration.FULL);
    DatasetEntityResponse finalResponse = finalResponseFuture.get();

    assertEquals(response1, finalResponse);
  }

  @Test
  public void retryNoAwait() throws Exception {
    when(underlying.sendMessage(message1, SendConfiguration.FULL)).thenReturn(CompletableFuture.completedFuture(response1));
    when(underlying.sendMessage(message2, SendConfiguration.FULL)).thenReturn(CompletableFuture.completedFuture(response2));
    when(assessor.getRetryMessage(message1, response1)).thenReturn(message2);
    when(assessor.getRetryMessage(message2, response2)).thenReturn(null);

    RetryMessageSender sender = new RetryMessageSender(underlying, assessor);
    Future<DatasetEntityResponse> finalResponseFuture = sender.sendMessage(message1, SendConfiguration.FULL);
    DatasetEntityResponse finalResponse = finalResponseFuture.get();

    assertEquals(response2, finalResponse);
  }
}
