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
package com.terracottatech.store.server.management;

import com.terracottatech.store.server.stream.PipelineProcessor;

import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToLongFunction;

public final class StreamStatistics {

  private final Queue<Partition> archive = new ConcurrentLinkedQueue<>();
  private final AtomicReference<Partition> activePartition;

  private volatile long windowSize;
  private volatile long partitionSize;

  StreamStatistics(Duration period, float accuracy, PipelineProcessor stream) {
    int partitionCount = (int) (1.0 / accuracy);
    this.windowSize = period.toNanos();
    this.partitionSize = windowSize / partitionCount;
    this.activePartition = new AtomicReference<>(new Partition(System.nanoTime(), partitionSize));
    try {
      increment(stream);
    } catch (IllegalStateException e) {
      throw new AssertionError(e);
    }
  }

  void increment(PipelineProcessor stream) throws IllegalStateException {
    while (true) {
      long time = System.nanoTime();
      Partition partition = activePartition.get();
      if (partition == null) {
        throw new IllegalStateException("Expired Statistic");
      } else if (partition.targetFor(time)) {
        partition.increment(stream);
        return;
      } else {
        Partition newPartition = new Partition(time, partitionSize);
        if (activePartition.compareAndSet(partition, newPartition)) {
          archive(partition);
          newPartition.increment(stream);
          return;
        }
      }
    }
  }

  boolean tryExpire() {
    final long startTime = System.nanoTime() - windowSize;

    Partition current = activePartition.get();

    if (current.isBefore(startTime)) {
      return activePartition.compareAndSet(current, null);
    } else if (current.getCount() > 0) {
      return false;
    }

    for (Partition p : archive) {
      if (!p.isBefore(startTime) && p.getCount() > 0) {
        return false;
      }
    }

    return activePartition.compareAndSet(current, null);
  }

  long getExecutionCount() {
    return sumInWindow(Partition::getCount);
  }

  long getTotalTime() {
    return sumInWindow(Partition::getTotalTime);
  }

  long getServerTime() {
    return sumInWindow(Partition::getServerTime);
  }

  public String toString() {
    return "Statistics: executions=" + getExecutionCount() + " total-time=" + getTotalTime() + " server-time=" + getServerTime();
  }

  private long sumInWindow(ToLongFunction<Partition> function) {
    final long endTime = System.nanoTime();
    final long startTime = endTime - windowSize;

    Partition current = activePartition.get();
    if (current.isBefore(startTime)) {
      return 0L;
    }

    long sum = function.applyAsLong(current);

    for (Iterator<Partition> it = archive.iterator(); it.hasNext(); ) {
      Partition partition = it.next();
      if (partition == current) {
        break;
      } else if (partition.isBefore(startTime)) {
        it.remove();
      } else {
        sum += function.applyAsLong(partition);
      }
    }

    return sum;
  }

  private void archive(Partition partition) {
    archive.add(partition);

    long startTime = partition.end() - windowSize;
    for (Partition earliest = archive.peek(); earliest != null && earliest.isBefore(startTime); earliest = archive.peek()) {
      if (archive.remove(earliest)) {
        break;
      }
    }
  }

  static class Partition {

    private final long end;

    private final LongAdder count = new LongAdder();
    private final LongAdder serverTime = new LongAdder();
    private final LongAdder totalTime = new LongAdder();


    Partition(long start, long length) {
      this.end = start + length;
    }

    public boolean targetFor(long time) {
      return end > time;
    }

    public boolean isBefore(long time) {
      return end < time;
    }

    public long end() {
      return end;
    }

    public void increment(PipelineProcessor stream) {
      count.increment();
      serverTime.add(stream.getServerTime());
      totalTime.add(stream.getTotalTime());
    }

    public long getCount() {
      return count.sum();
    }

    public long getServerTime() {
      return serverTime.sum();
    }

    public long getTotalTime() {
      return totalTime.sum();
    }
  }
}
