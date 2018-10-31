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
import org.terracotta.lease.connection.TimeBudget;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class RetryFuture<R extends DatasetEntityResponse> implements Future<R> {
  private final MessageSender messageSender;
  private final SendConfiguration configuration;
  private final RetryMessageAssessor retryMessageAssessor;
  private volatile int messageCount;
  private volatile DatasetEntityMessage latestMessage;
  private final AtomicReference<CompletableFuture<R>> resultReference = new AtomicReference<>();
  private volatile Future<R> responseFuture;
  private volatile boolean done;

  public RetryFuture(MessageSender messageSender, SendConfiguration configuration, RetryMessageAssessor retryMessageAssessor, DatasetEntityMessage message) {
    this.messageSender = messageSender;
    this.configuration = configuration;
    this.retryMessageAssessor = retryMessageAssessor;
    this.messageCount = 0;
    sendMessage(message);
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return done;
  }

  @Override
  public R get() throws InterruptedException, ExecutionException {
    try {
      return get(new NoTimeoutFutureValueReader<>());
    } catch (TimeoutException e) {
      throw new AssertionError("TimeoutException should not have been thrown by NoTimeoutFutureValueReader", e);
    }
  }

  @Override
  public R get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return get(new TimeBudgetFutureValueReader<>(new TimeBudget(timeout, unit)));
  }

  private R get(FutureValueReader<R> futureValueReader) throws InterruptedException, ExecutionException, TimeoutException {
    if (responseFuture == null) {
      throw new AssertionError("We are not waiting for a response, but we should be");
    }

    CompletableFuture<R> newResult;
    while (true) {
      newResult = new CompletableFuture<>();
      CompletableFuture<R> possibleNewResult = newResult; // We need an "effectively final" variable for the lambda
      CompletableFuture<R> existingResult = resultReference.getAndUpdate(existing -> existing != null ? existing : possibleNewResult);

      if (existingResult == null) {
        break;
      }

      try {
        return futureValueReader.apply(existingResult);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof ThreadResignationException) {
          // The thread that was populating existingResult gave up (interrupted / timed out)
          // So let's loop and try to take responsibility
          resultReference.compareAndSet(existingResult, null);
        } else {
          throw e;
        }
      }
    }

    // Due to the previous logic, only a single thread can be running the following loop at any one time
    try {
      DatasetEntityResponse latestResponse;
      while (true) {
        latestResponse = futureValueReader.apply(responseFuture);

        DatasetEntityMessage retryMessage = retryMessageAssessor.getRetryMessage(latestMessage, latestResponse);
        if (retryMessage == null) {
          break;
        }

        sendMessage(retryMessage);
      }
      @SuppressWarnings("unchecked")
      R castResponse = (R) latestResponse;
      newResult.complete(castResponse);
      done = true;

      return castResponse;
    } catch (InterruptedException | TimeoutException e) {
      newResult.completeExceptionally(new ThreadResignationException(e));
      throw e;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      newResult.completeExceptionally(cause);
      done = true;
      throw e;
    }
  }

  /*
   * This method should only be called from a single thread at a time.
   * That is the case because it is only called from the constructor and the getRetryMessage loop in get().
   */
  private void sendMessage(DatasetEntityMessage message) {
    incrementMessageCount();
    latestMessage = message;
    responseFuture = messageSender.sendMessage(message, configuration);

    if (responseFuture == null) {
      throw new AssertionError("MessageSender.sendMessage returned null");
    }
  }

  /*
   * This method should only be called from a single thread at a time.
   * That is the case because it is only called via sendMessage() which is only called
   * from the constructor and the getRetryMessage loop in get().
   */
  private void incrementMessageCount() {
    // This clumsy form of messageCount++ stops findbugs complaining
    int incremented = messageCount + 1;
    messageCount = incremented;
  }

  private interface FutureValueReader<X> {
    X apply(Future<X> future) throws InterruptedException, ExecutionException, TimeoutException;
  }

  private static class NoTimeoutFutureValueReader<X> implements FutureValueReader<X> {
    @Override
    public X apply(Future<X> future) throws InterruptedException, ExecutionException {
      return future.get();
    }
  }

  private static class TimeBudgetFutureValueReader<X> implements FutureValueReader<X> {
    private final TimeBudget timeBudget;

    public TimeBudgetFutureValueReader(TimeBudget timeBudget) {
      this.timeBudget = timeBudget;
    }

    @Override
    public X apply(Future<X> future) throws InterruptedException, ExecutionException, TimeoutException {
      long timeout = timeBudget.remaining(TimeUnit.MILLISECONDS);

      if (timeout <= 0) {
        throw new TimeoutException();
      }

      return future.get(timeout, TimeUnit.MILLISECONDS);
    }
  }
}
