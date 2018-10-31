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
package com.terracottatech.store.server.execution;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * A {@link ThreadPoolExecutor} to support element-at-a-time processing from a
 * {@link java.util.Spliterator Spliterator} backed by a {@link java.util.stream.Stream Stream} taken
 * from a {@link com.terracottatech.sovereign.SovereignDataset SovereignDataset}. The
 * {@code ThreadPoolExecutor} does not queue and will try its best effort to handle all requests until
 * a hard limit (almost always imposed by the OS) is reached. To prevent DoS'ing the process, only up to
 * {@code maxPoolSize} threads will be kept in the pool, every other execution request will be serviced by
 * a freshly spawned thread that will die immediately after its job is done.
 */
public class PipelineProcessorExecutor extends ThreadPoolExecutor {

  private static final LongAdder GLOBAL_STREAM_COUNTER = new LongAdder();

  private final LongAdder localStreamCounter = new LongAdder();
  private final OverflowPolicy overflowPolicy;

  public PipelineProcessorExecutor(String datasetId, long keepAliveTime, TimeUnit unit, int maxPoolSize) {
    this(datasetId, keepAliveTime, unit, maxPoolSize, new LocalThreadFactory(datasetId));
  }

  PipelineProcessorExecutor(String datasetId, long keepAliveTime, TimeUnit unit, int maxPoolSize, ThreadFactory threadFactory) {
    super(0, maxPoolSize, keepAliveTime, unit, new SynchronousQueue<>(), threadFactory, new ThreadPoolExecutor.AbortPolicy());
    this.overflowPolicy = new OverflowPolicy(datasetId, localStreamCounter);
    setRejectedExecutionHandler(overflowPolicy);
  }

  private static class LocalThreadFactory implements ThreadFactory {

    private static final ThreadGroup THREAD_GROUP = new ThreadGroup("TCStore PipelineProcessor Workers");

    private final AtomicLong threadId = new AtomicLong(0);
    private final String instanceId;

    private LocalThreadFactory(String instanceId) {
      this.instanceId = instanceId;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(THREAD_GROUP, r,
          "PipelineProcessor:" + instanceId + ":" + threadId.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    }
  }

  @Override
  public void execute(Runnable command) {
    Runnable task = () -> {
      try {
        localStreamCounter.increment();
        GLOBAL_STREAM_COUNTER.increment();
        command.run();
      } finally {
        localStreamCounter.decrement();
        GLOBAL_STREAM_COUNTER.decrement();
      }
    };

    super.execute(task);
  }

  @Override
  public List<Runnable> shutdownNow() {
    List<Runnable> runnables = super.shutdownNow();
    overflowPolicy.interrupt();
    return runnables;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long end = System.nanoTime() + unit.toNanos(timeout);
    return overflowPolicy.awaitTermination(timeout, unit) && super.awaitTermination(end - System.nanoTime(), TimeUnit.NANOSECONDS);
  }

  /**
   * A RejectedExecutionHandler whose policy is to spawn non-pooled threads
   * to handle tasks that the pool rejects.
   */
  static class OverflowPolicy implements RejectedExecutionHandler {
    private final Set<Thread> overflowThreads = ConcurrentHashMap.newKeySet();
    private final String datasetId;
    private final Number localStreamCounter;

    OverflowPolicy(String datasetId, Number localStreamCounter) {
      this.datasetId = datasetId;
      this.localStreamCounter = localStreamCounter;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      try {
        Thread thread = executor.getThreadFactory().newThread(() -> {
          Thread current = Thread.currentThread();
          overflowThreads.add(current);
          try {
            r.run();
          } finally {
            overflowThreads.remove(current);
          }
        });
        thread.start();
      } catch (Throwable t) {
        throw new RejectedExecutionException("Execution rejected for dataset '" + datasetId + "' - already handling " +
            localStreamCounter.longValue() + " stream(s) of " + GLOBAL_STREAM_COUNTER.longValue() + " total stream(s)", t);
      }
    }

    void interrupt() {
      overflowThreads.forEach(Thread::interrupt);
    }

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      long end = System.nanoTime() + unit.toNanos(timeout);
      for (Thread overflowThread : overflowThreads) {
        long wait = end - System.nanoTime();
        if (wait <= 0L) {
          break;
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(wait);
        int nanos = (int) (wait - TimeUnit.MILLISECONDS.toNanos(millis));
        overflowThread.join(millis, nanos);
      }

      return overflowThreads.stream().noneMatch(Thread::isAlive);
    }
  }
}
