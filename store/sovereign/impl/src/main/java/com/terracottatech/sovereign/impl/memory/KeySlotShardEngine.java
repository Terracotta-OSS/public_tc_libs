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
package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.common.utils.MiscUtils;

/**
 * Sharding engine for long values and keys.
 *
 * @author cschanck
 **/
public class KeySlotShardEngine {

  @FunctionalInterface
  public static interface LongToLong {
    long transmute(long in);
  }

  @FunctionalInterface
  public static interface LongToInt {
    int transmute(long in);
  }

  private final int shardCount;
  private final int hashMask;
  private final int bitsize;
  private final int bitPosition;
  private final long bitMask;
  private final LongToLong removeMaskLambda;
  private final LongToInt extractShardIndex;

  public KeySlotShardEngine(int concurrency) {
    this(61, Integer.bitCount(concurrency - 1));
  }

  KeySlotShardEngine(int bitPosition, int bitsize) {
    bitsize = Math.min(bitsize, 16);
    bitPosition=Math.min(61, bitPosition);
    if (bitPosition <= bitsize) {
      throw new IllegalArgumentException("Bit position must be > bit size: " + bitPosition + "/" + bitsize);
    }
    this.bitsize = bitsize;
    this.bitPosition = bitPosition;
    long mask = MiscUtils.maskForBitsize(bitsize);
    this.hashMask = (int) mask;
    if (bitsize == 0) {
      // ok, special case.
      this.bitMask = (int) mask;
      this.removeMaskLambda = in -> in;
      this.extractShardIndex = in -> 0;
      this.shardCount = 1;
    } else {
      // need the mask and the shift amount.
      this.bitMask = mask << (bitPosition - bitsize + 1);
      this.removeMaskLambda = in -> in & (~bitMask);
      this.extractShardIndex = in -> {
        long m = in & KeySlotShardEngine.this.bitMask;
        m = m >> (KeySlotShardEngine.this.bitPosition - KeySlotShardEngine.this.bitsize + 1);
        return (int) m;
      };
      this.shardCount = 1 << bitsize;
    }
  }

  public LongToInt extractShardIndexLambda() {
    return extractShardIndex;
  }

  public LongToLong addShardLambdaFor(int index) {
    return (l) -> {
      long m = index;
      m = m << (bitPosition - bitsize + 1);
      m = m & bitMask;
      return l | m;
    };
  }

  public LongToLong removeShardIndexLambda() {
    return removeMaskLambda;
  }

  public int getShardCount() {
    return shardCount;
  }

  public long getBitMask() {
    return bitMask;
  }

  public int getBitPosition() {
    return bitPosition;
  }

  public int getBitsize() {
    return bitsize;
  }

  public int shardIndexForKey(Object k) {
    return MiscUtils.stirHash(k.hashCode()) & hashMask;
  }

  public static final LongToLong NOOP_LTOL = (l) -> {
    return l;
  };

}
