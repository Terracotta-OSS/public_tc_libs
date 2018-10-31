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

import com.terracottatech.sovereign.impl.memory.KeySlotShardEngine;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToIntFunction;

/**
 * @author cschanck
 **/
public class LockSet {

  public static final ToIntFunction<Object> DEFAULT_HASHCODE = obj -> {
    // stolen from CHM
    int h = obj.hashCode();
    h += (h << 15) ^ 0xffffcd7d;
    h ^= (h >>> 10);
    h += (h << 3);
    h ^= (h >>> 6);
    h += (h << 2) + (h << 14);
    return h ^ (h >>> 16);
  };

  private final ReentrantLock[] locks;
  private final int shift;
  private final ToIntFunction<Object> keySelector;
  private final int size;
  private final boolean isFair;

  public LockSet(int many, boolean isFair) {
    this(many, DEFAULT_HASHCODE, isFair);
  }

  public LockSet(int many, ToIntFunction<Object> keySelector, boolean isFair) {
    this.keySelector = keySelector;
    this.isFair = isFair;
    int shift = 0;
    int sz = 1;
    while (sz < many) {
      shift++;
      sz <<= 1;
    }
    this.size = sz;
    locks = new ReentrantLock[size];
    this.shift = 32 - shift;
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantLock(isFair);
    }
  }

  public LockSet(KeySlotShardEngine engine, boolean isFair) {
    this(engine.getShardCount(), (o) -> {
      return engine.shardIndexForKey(o);
    }, isFair);
  }

  private int index(Object k) {
    return keySelector.applyAsInt(k) & locks.length - 1;
  }

  public ReentrantLock lockFor(Object k) {
    return locks[index(k)];
  }

  public void lockAll() {
    for (ReentrantLock l : locks) {
      l.lock();
    }
  }

  public void unlockAll() {
    for (ReentrantLock l : locks) {
      l.unlock();
    }
  }

  public int getSize() {
    return size;
  }

  public boolean isFair() {
    return isFair;
  }
}
