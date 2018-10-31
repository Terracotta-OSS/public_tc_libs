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
package com.terracottatech.sovereign.impl.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * The idea is that the smallest interval of timed syncing is the baseline multiple,
 * all longer (laxer) flushing is done as multiples of this baseline. So if you have
 * datasets that have timeds durability of 10ms, 100ms, 15ms, they will happen at
 * 10ms (1 multiple), 100ms (10 multiples), 10ms (15/10 == 1 multiple). This gives us
 * per-dataset granularity on timed fsync, but is conservative and low overhead, with only 1
 * timer running for all the storage engines datasets.
 */
public class SharedSyncher {
  private static Logger LOG = LoggerFactory.getLogger(SharedSyncher.class);
  private final ScheduledExecutorService ses = newSingleThreadScheduledExecutor(new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r, "Sync Thread");
      t.setDaemon(true);
      return t;
    }
  });
  private Runnable task;
  private final AtomicLong countdown = new AtomicLong(0);
  private final AtomicLong runCounter = new AtomicLong(0);
  private volatile Runnable delegate;
  private final AtomicLong multipleLengthNS = new AtomicLong(0);
  private final AtomicLong version = new AtomicLong(0);
  private volatile boolean dead = false;
  private volatile ScheduledFuture<?> runningFuture;
  private Lock lock = new StampedLock().asWriteLock();
  private Map<SyncRequest, Long> nanos = new ConcurrentHashMap<>();

  public SharedSyncher(Runnable delegate) {
    this.delegate = delegate;
    this.task = () -> {
      if (!dead) {
        try {
          if (countdown.decrementAndGet() == 0) {
            //System.out.println("syncing");
            delegate.run();
            runCounter.incrementAndGet();
          }
        } catch (Throwable t) {
          if (!dead) {
            LOG.warn("Error:", t);
          }
        }
      }
    };
  }

  public long getRunCounter() {
    return runCounter.get();
  }

  public int getLiveRequestCount() {
    return this.nanos.size();
  }

  public long getCurrentMultipleLengthNS() {
    return multipleLengthNS.get();
  }

  private long calculateMultiple(long dur, TimeUnit units) {
    return Math.max(1, TimeUnit.NANOSECONDS.convert(dur, units) / multipleLengthNS.get());
  }

  public SyncRequest fetchSyncRequest(long dur, TimeUnit units) {
    return new SyncRequest(dur, units);
  }

  public void requestImmediate() {
    if (!dead) {
      countdown.set(0);
      try {
        delegate.run();
        runCounter.incrementAndGet();
      } catch (Throwable t) {
        if (!dead) {
          LOG.warn("Error:", t);
        }
      }
    }
  }

  private void requestSync(SyncRequest req) {
    if (req.getLastVersion() < version.get()) {
      lock.lock();
      try {
        req.restrike();
      } finally {
        lock.unlock();
      }
    }
    while (true) {
      long probe = countdown.get();
      long m = req.getMultiple();
      if (probe > 0 && probe < m) {
        //System.out.println("Skipping " + multiple + " vs " + probe);
        return;
      }
      if (countdown.compareAndSet(probe, m)) {
        //System.out.println("Set to " + multiple);
        return;
      }
    }
  }

  private void updateScheduleAdd(SyncRequest request) {
    lock.lock();
    try {
      checkDead();
      this.nanos.put(request, request.getUnits().toNanos(request.getDuration()));
      updateSchedule();
    } finally {
      lock.unlock();
    }
  }

  private void updateScheduleRemove(SyncRequest request) {
    lock.lock();
    try {
      this.nanos.remove(request);
      updateSchedule();
    } catch (Exception e) {
      if (!dead) {
        throw new IllegalStateException(e);
      }
    } finally {
      lock.unlock();
    }
  }

  private void updateSchedule() {
    // first shutdown
    cancelIfRunning();

    Optional<Long> first = nanos.values().stream().sorted().findFirst();

    // now schedule
    if (first.isPresent()) {
      this.multipleLengthNS.set(first.get());
      this.runningFuture = ses.scheduleAtFixedRate(task,
                                                   multipleLengthNS.get(),
                                                   multipleLengthNS.get(),
                                                   TimeUnit.NANOSECONDS);
      LOG.debug("Shared Sync updated interval: {0}", this.multipleLengthNS);
    } else {
      this.multipleLengthNS.set(0l);
      LOG.debug("Shared Sync updated interval: {0}", this.multipleLengthNS);
    }
    version.incrementAndGet();
  }

  private void checkDead() {
    if (dead) {
      throw new IllegalStateException();
    }
  }

  private void cancelIfRunning() {
    if (runningFuture != null) {
      runningFuture.cancel(false);
      runningFuture = null;
    }
  }

  public void stop() {
    lock.lock();
    try {
      if (!dead) {
        dead = true;
        cancelIfRunning();
        ses.shutdownNow();
        this.nanos = Collections.unmodifiableMap(Collections.emptyMap());
        this.delegate = null;       // Drop indirect reference to off-heap storage
        this.task = null;           // Drop indirect reference to off-heap storage
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * This class represents a request for periodic syncing. It is made in
   * terms of time, which is rendered into multiples of the current
   * period of syncing.
   */
  public class SyncRequest {
    private final long duration;
    private final TimeUnit units;
    private long multiple;
    private long lastVersion;
    private volatile boolean live = true;

    private SyncRequest(long duration, TimeUnit units) {
      this.lastVersion = version.get();
      this.duration = duration;
      this.units = units;
      updateScheduleAdd(this);
      this.multiple = calculateMultiple(duration, units);
    }

    public long getDuration() {
      return duration;
    }

    public TimeUnit getUnits() {
      return units;
    }

    private long getMultiple() {
      return multiple;
    }

    private long getLastVersion() {
      return lastVersion;
    }

    private void restrike() {
      lastVersion = version.get();
      this.multiple = calculateMultiple(duration, units);
    }

    public void request() {
      SharedSyncher.this.requestSync(this);
    }

    public synchronized void release() {
      if (live) {
        updateScheduleRemove(this);
        live = false;
      }
    }

    @Override
    public String toString() {
      return "SyncRequest{" + "duration=" + duration + ", units=" + units + '}';
    }
  }
}
