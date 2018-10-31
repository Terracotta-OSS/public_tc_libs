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

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.ToIntFunction;

/**
 * @author cschanck
 **/
public class RWLockSet {

  private final ReentrantReadWriteLock[] locks;
  private final int shift;
  private final ToIntFunction<Object> keySelector;
  private final int size;
  private final boolean isFair;

  public RWLockSet(int many, boolean isFair) {
    this(many, LockSet.DEFAULT_HASHCODE, isFair);
  }

  public RWLockSet(int many, ToIntFunction<Object> keySelector, boolean isFair) {
    this.keySelector = keySelector;
    this.isFair = isFair;
    int shift = 0;
    int sz = 1;
    while (sz < many) {
      shift++;
      sz <<= 1;
    }
    this.size = sz;
    locks = new ReentrantReadWriteLock[size];
    this.shift = 32 - shift;
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantReadWriteLock(isFair);
    }
  }

  private int index(Object k) {
    return keySelector.applyAsInt(k) & locks.length - 1;
  }

  public ReentrantReadWriteLock lockFor(Object k) {
    return locks[index(k)];
  }

  public ReentrantReadWriteLock.ReadLock readLockFor(Object k) {
    return lockFor(k).readLock();
  }

  public ReentrantReadWriteLock.WriteLock writeLockFor(Object k) {
    return lockFor(k).writeLock();
  }

  public int getSize() {
    return size;
  }

  public boolean isFair() {
    return isFair;
  }
}
