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
import com.terracottatech.store.common.messages.crud.AddRecordMessage;
import com.terracottatech.store.common.messages.crud.AddRecordSimplifiedResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static com.terracottatech.store.client.message.SendConfiguration.FULL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RetryFutureTest {
  @Mock
  private AddRecordMessage<Integer> message1;

  @Mock
  private AddRecordSimplifiedResponse response1;

  @Mock
  private AddRecordMessage<Integer> message2;

  @Mock
  private AddRecordSimplifiedResponse response2;

  @Mock
  private MessageSender messageSender;

  @Mock
  private RetryMessageAssessor retryMessageAssessor;

  private CompletableFuture<DatasetEntityResponse> responseFuture1 = new CompletableFuture<>();
  private CompletableFuture<DatasetEntityResponse> responseFuture2 = new CompletableFuture<>();
  private RetryFuture<AddRecordSimplifiedResponse> retryFuture;
  private Thread thread1;
  private Thread thread2;

  @Before
  public void before() {
    when(messageSender.sendMessage(message1, FULL)).thenReturn(responseFuture1);
    when(messageSender.sendMessage(message2, FULL)).thenReturn(responseFuture2);

    retryFuture = new RetryFuture<>(messageSender, FULL, retryMessageAssessor, message1);

    verify(messageSender, times(1)).sendMessage(message1, FULL);
  }

  @Test
  public void simpleGet() throws Exception {
    when(retryMessageAssessor.getRetryMessage(message1, response1)).thenReturn(null);

    responseFuture1.complete(response1);
    AddRecordSimplifiedResponse result = retryFuture.get();
    assertEquals(response1, result);
  }

  @Test
  public void twoThreadsGet() throws Exception {
    when(retryMessageAssessor.getRetryMessage(message1, response1)).thenReturn(null);

    List<Throwable> throwables = startGetThreads();

    responseFuture1.complete(response1);

    thread1.join();
    thread2.join();

    if (!throwables.isEmpty()) {
      throw new RuntimeException(throwables.get(0));
    }

    verify(messageSender, times(1)).sendMessage(message1, FULL);
  }

  @Test
  public void twoThreadsGetOneInterrupted() throws Exception {
    when(retryMessageAssessor.getRetryMessage(message1, response1)).thenReturn(null);

    List<Throwable> throwables = startGetThreads();

    thread1.interrupt();
    boolean condition = awaitCondition(20, TimeUnit.SECONDS, () -> throwables.size() > 0);
    assertTrue(condition);
    responseFuture1.complete(response1);

    thread1.join();
    thread2.join();

    for (Throwable throwable : throwables) {
      if (!(throwable instanceof RuntimeException)) {
        throw new RuntimeException(throwable);
      }

      Throwable cause = throwable.getCause();
      assertNotNull(cause);

      if (!(cause instanceof InterruptedException)) {
        throw (RuntimeException) throwable;
      }
    }

    assertEquals(1, throwables.size());

    verify(messageSender, times(1)).sendMessage(message1, FULL);
  }

  @Test
  public void simpleRetry() throws Exception {
    when(retryMessageAssessor.getRetryMessage(message1, response1)).thenReturn(message2);
    when(retryMessageAssessor.getRetryMessage(message2, response2)).thenReturn(null);

    responseFuture1.complete(response1);
    responseFuture2.complete(response2);
    AddRecordSimplifiedResponse result = retryFuture.get();
    assertEquals(response2, result);

    verify(messageSender, times(1)).sendMessage(message1, FULL);
    verify(messageSender, times(1)).sendMessage(message2, FULL);
  }

  private boolean awaitCondition(int timeout, TimeUnit unit, BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, unit);
    while (System.nanoTime() < deadline && !condition.getAsBoolean()) {
      Thread.sleep(50);
    }

    return condition.getAsBoolean();
  }

  private List<Throwable> startGetThreads() {
    Runnable getRunnable = () -> {
      try {
        AddRecordSimplifiedResponse result = retryFuture.get();
        assertEquals(response1, result);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    List<Throwable> threadExceptions = new CopyOnWriteArrayList<>();
    Thread.UncaughtExceptionHandler uncaughtExceptionHandler = (thread, throwable) -> threadExceptions.add(throwable);

    thread1 = new Thread(getRunnable);
    thread2 = new Thread(getRunnable);

    thread1.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    thread2.setUncaughtExceptionHandler(uncaughtExceptionHandler);

    thread1.start();
    thread2.start();

    return threadExceptions;
  }
}
