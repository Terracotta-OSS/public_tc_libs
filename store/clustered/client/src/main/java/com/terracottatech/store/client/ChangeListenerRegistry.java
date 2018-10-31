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
package com.terracottatech.store.client;

import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.ChangeType;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.client.message.MessageSender;
import com.terracottatech.store.client.message.SendConfiguration;
import com.terracottatech.store.common.ExceptionFreeAutoCloseable;
import com.terracottatech.store.common.InterruptHelper;
import com.terracottatech.store.common.messages.event.SendChangeEventsMessage;
import com.terracottatech.store.common.messages.SuccessResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class ChangeListenerRegistry<K extends Comparable<K>> implements ChangeListener<K>, ExceptionFreeAutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeListenerRegistry.class);
  private static final int MAXIMUM_LISTENER_BACKLOG = 10_000;
  private static final long DEREGISTRATION_MAX_WAIT = 30;

  private final MessageSender messageSender;
  private final ExecutorService eventStatusThread = Executors.newSingleThreadExecutor();
  private final Map<ChangeListener<K>, ExecutorService> changeListeners = new ConcurrentHashMap<>();
  private volatile boolean shutdown;

  // This eventStatus boolean is only accessed via the eventStatusThread - a single threaded executor.
  // But an executor created via newSingleThreadExecutor may use more than one thread.
  // For example, if the thread dies then executor will create another.
  // But the executor provides a guarantee of sequential operation, so there is no need to ensuring ordering on eventStatus
  // by using volatile / synchronized / etc.
  private boolean eventStatus;

  public ChangeListenerRegistry(MessageSender messageSender) {
    this.messageSender = messageSender;
  }

  public boolean sendChangeEvents() {
    return !changeListeners.isEmpty();
  }

  public void registerChangeListener(ChangeListener<K> listener) {
    ExecutorService eventFiringThread = createEventFiringThread(listener);
    changeListeners.put(listener, eventFiringThread);

    updateEventStatusAndWait();
  }

  public void deregisterChangeListener(ChangeListener<K> listener) {
    ExecutorService eventFiringThread = changeListeners.remove(listener);
    eventFiringThread.shutdown();
    long firingTimeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(DEREGISTRATION_MAX_WAIT);
    boolean interrupted = false;
    try {
      while (true) {
        try {
          if (!eventFiringThread.awaitTermination(firingTimeout - System.nanoTime(), TimeUnit.SECONDS)) {
            LOGGER.warn("Failed to complete firing the event backlog within " + DEREGISTRATION_MAX_WAIT +
                    "seconds - outstanding event firing will complete asynchronously");
          }
          break;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    updateEventStatusAndWait();
  }

  private void updateEventStatusAndWait() {
    Future<Optional<SuccessResponse>> updateResponse = updateEventStatus();

    try {
      InterruptHelper.getUninterruptibly(updateResponse);
    } catch (ExecutionException e) {
      throw new StoreRuntimeException(e.getCause());
    }
  }

  private synchronized Future<Optional<SuccessResponse>> updateEventStatus() {
    boolean sendChangeEvents = sendChangeEvents();
    return eventStatusThread.submit(() -> {
      if (eventStatus != sendChangeEvents) {
        eventStatus = sendChangeEvents;
        SendChangeEventsMessage message = new SendChangeEventsMessage(sendChangeEvents);
        return Optional.of(messageSender.sendMessageAwaitResponse(message, SendConfiguration.ONE_SERVER));
      }

      return Optional.empty();
    });
  }

  @Override
  public void onChange(K key, ChangeType changeType) {
    if (!shutdown) {
      fireEventForListeners(listener -> listenerOnChange(listener, key, changeType));
    }
  }

  @Override
  public void missedEvents() {
    if (!shutdown) {
      fireEventForListeners(this::listenerMissedEvents);
    }
  }

  private void fireEventForListeners(Consumer<ChangeListener<K>> eventFiring) {
    changeListeners.forEach((listener, eventFiringThread) -> eventFiringThread.submit(() -> {
      try {
        eventFiring.accept(listener);
      } catch (RuntimeException e) {
        LOGGER.error("Error in ChangeListener: " + listener, e);
      }
    }));
  }

  private void listenerOnChange(ChangeListener<K> listener, K key, ChangeType changeType) {
    handleError(listener, listen -> listen.onChange(key, changeType));
  }

  private void listenerMissedEvents(ChangeListener<K> listener) {
    handleError(listener, ChangeListener::missedEvents);
  }

  private void handleError(ChangeListener<K> listener, Consumer<ChangeListener<K>> action) {
    try {
      action.accept(listener);
    } catch (RuntimeException e) {
      LOGGER.error("Error in ChangeListener: " + listener, e);
    }
  }

  public void shutdown(long timeoutMilliseconds) {
    shutdown = true;

    forAllExecutorServices(ExecutorService::shutdown);

    forAllExecutorServices(executor -> {
      try {
        executor.awaitTermination(timeoutMilliseconds, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.error("Shutdown interrupted", e);
      }
    });
  }

  @Override
  public void close() {
    shutdown = true;
    forAllExecutorServices(ExecutorService::shutdownNow);
  }

  private void forAllExecutorServices(Consumer<ExecutorService> operation) {
    operation.accept(eventStatusThread);

    changeListeners.forEach((listener, eventFiringThread) -> {
      operation.accept(eventFiringThread);
    });

  }

  private ThreadPoolExecutor createEventFiringThread(ChangeListener<K> listener) {
    RejectedExecutionHandler rejectHandler = (runnable, executor) -> {
      if (!executor.isShutdown()) {
        executor.getQueue().clear();

        executor.submit(() -> {
          listenerMissedEvents(listener);
        });
      }
    };

    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(MAXIMUM_LISTENER_BACKLOG);
    ThreadFactory threadFactory = new EventFireThreadFactory();
    return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queue, threadFactory, rejectHandler);
  }

  private static class EventFireThreadFactory implements ThreadFactory {
    private static final AtomicLong count = new AtomicLong(1);

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable);
      thread.setName("event-fire-" + count.getAndIncrement());
      return thread;
    }
  }
}
