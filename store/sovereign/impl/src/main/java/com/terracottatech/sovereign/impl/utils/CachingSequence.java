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
package com.terracottatech.sovereign.impl.utils;

import com.terracottatech.sovereign.common.utils.MiscUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class CachingSequence implements Externalizable {
  public static final String SOVEREIGN_USE_LONGMIN_SEQUENCE_SEED_PROP = "sovereign.use_longmin_sequence_seed";
  private static final boolean USE_MIN_SEED = MiscUtils.getBoolean(SOVEREIGN_USE_LONGMIN_SEQUENCE_SEED_PROP, true);
  private static final Logger LOG = LoggerFactory.getLogger(CachingSequence.class);

  static {
    if (!USE_MIN_SEED) {
      LOG.warn("Using non default ZERO caching sequence seed. (" + SOVEREIGN_USE_LONGMIN_SEQUENCE_SEED_PROP + ")");
    }
  }

  private static final long serialVersionUID = 5366602708637199013L;
  private final ReentrantLock lock = new ReentrantLock();
  private volatile int cacheMany;
  private AtomicLong nextSequence = new AtomicLong(0);
  private volatile long nextChunk = 0;

  private transient Consumer<CachingSequence> callback = sequence -> {
    throw new UnsupportedOperationException();
  };

  public CachingSequence() {
    this(USE_MIN_SEED ? Long.MIN_VALUE : 0, 1000);
  }

  public CachingSequence(long seed, int cacheMany) {
    nextSequence.set(seed);
    nextChunk = seed;
    this.cacheMany = cacheMany;
  }

  public final void setCallback(Consumer<CachingSequence> callback) {
    this.callback = callback;
  }

  public long current() {
    return nextSequence.get();
  }

  public int getCacheMany() {
    return cacheMany;
  }

  public long getNextChunk() {
    return nextChunk;
  }

  public long next() {
    long ret = nextSequence.incrementAndGet();
    if (ret >= nextChunk) {
      lock.lock();
      try {
        while (ret >= nextChunk) {
          nextChunk = nextChunk + cacheMany;
          callback.accept(this);
        }
      } finally {
        lock.unlock();
      }
    }
    return ret;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // Do not acquire the lock here because otherwise it creates a deadlock between:
    //   A: next() calling callback which writes to a RestartableMap and tries to acquire its lock
    //   B: Compaction locking the RestartableMap and then calling writeExternal()
    // See TDB-2588 for more details.

    // We do not need a lock because:
    //  * nextChunk is volatile
    //  * whilst cacheMany is volatile, this is only to provide happens-before semantics needed after object creation
    //    which might happen via the constructor (cacheMany is not final) or via readExternal()
    //  * cacheMany cannot change after object creation, so there is no relationship between the two variables for a
    //    lock to protect
    //  * the out variable is not state shared with callback

    out.writeInt(cacheMany);
    out.writeLong(nextChunk);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    cacheMany = in.readInt();
    nextChunk = in.readLong();
    nextSequence.set(nextChunk);
  }
}
