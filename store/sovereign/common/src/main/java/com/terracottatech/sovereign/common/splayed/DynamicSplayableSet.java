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
package com.terracottatech.sovereign.common.splayed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author cschanck
 **/
public class DynamicSplayableSet<T extends DynamicSplayableSet.SplayedObject<T>> {

  /**
   * a managed splayed object.
   *
   * @param <TT>
   */
  public static interface SplayedObject<TT extends SplayedObject<TT>> {
    TT object();

    /**
     * Halve the current object, based on the specified hash function. if the hash anded
     * with 1 at the bitposition is 0, move it to a new instance and return that instance. Do
     * this for all elements. Else leave the element in place.
     *
     * @param hashFunc has function
     * @param bitPos bit position that signifies whether it stays or goes.
     * @return new set, where the bit at bitpos was 0.
     */
    TT halve(HashSpreadFunc hashFunc, int bitPos);

    /**
     * Is this managed object empty.
     *
     * @return
     */
    boolean isEmpty();

    /**
     * Is it oversized, in need of sharding.
     *
     * @return
     */
    boolean isOversized();
  }

  @FunctionalInterface
  public static interface HashSpreadFunc {
    int hash(int hashcode);
  }

  protected SplayedObject<T>[] set;
  private int shift;
  private final HashSpreadFunc hash;

  @SuppressWarnings("unchecked")
  public DynamicSplayableSet(HashSpreadFunc hash, T seed) {
    this.hash = hash;
    this.set = (SplayedObject<T>[]) new SplayedObject<?>[]{seed};
    this.shift = 32;
  }

  @SuppressWarnings("unchecked")
  protected void reset(T seed) {
    this.set = (SplayedObject<T>[]) new SplayedObject<?>[]{seed};
  }

  /**
   * Snapshot the list of shards.
   *
   * @return
   */
  @SuppressWarnings("unchecked")
  protected List<T> shards() {
    return new ArrayList<T>((Collection<? extends T>) Arrays.asList(set));
  }

  /**
   * return the shard at the index.
   *
   * @param index
   * @return
   */
  @SuppressWarnings("unchecked")
  protected T shardAt(int index) {
    return (T) set[index];
  }

  /**
   * Find the current shard an object hashes to. Small linear probe might be necessary.
   *
   * @param key
   * @return
   */
  protected int shardIndexFor(Object key) {
    int h = 0;
    if (shift < 32) {
      h = hash.hash(key.hashCode());
      h = h >>> shift;
    }
    while (set[h] == null) {
      h++;
    }
    return h;
  }

  /**
   * Actualy spread the set. Set is still correct, just doubled in size at this point.
   */
  protected void spreadSet() {
    @SuppressWarnings("unchecked")
    SplayedObject<T>[] tmp = (SplayedObject<T>[]) new SplayedObject<?>[set.length * 2];
    for (int i = 0; i < set.length; i++) {
      tmp[(i << 1) + 1] = set[i];
    }
    this.set = tmp;
    shift--;
  }

  /**
   * Here we halve the specified shard index. This might just call halve and fill an empty
   * shard, or it might cause a spread and a then a halve call.
   *
   * @param index
   */
  protected void halveAndPossiblySpread(int index) {
    if (set[index] == null) {
      throw new IllegalStateException();
    }
    boolean cleaned = false;
    for (int shiftAmount = set.length; shiftAmount > 0; shiftAmount = shiftAmount / 2) {
      int p = index - shiftAmount;
      if (p >= 0 && set[p] == null) {
        int bitPos = shift + Integer.numberOfTrailingZeros(shiftAmount);
        T newbie = set[index].halve(hash, bitPos);
        set[p] = newbie;
        cleaned = true;
        break;
      }
    }
    if (!cleaned) {
      spreadSet();
      index = index * 2 + 1;
      halveAndPossiblySpread(index);
    }
  }

  @Override
  public String toString() {
    return "SplayableSet{" +
      "shift=" + shift +
      ", set=" + Arrays.toString(set) +
      '}';
  }

  /**
   * Number of shards.
   *
   * @return
   */
  protected int size() {
    return set.length;
  }

  /**
   * Check if a shard is oversized, if so, halve and spread as needed.
   *
   * @param index
   * @return true if elements were moved.
   */
  protected boolean checkShard(int index) {
    if (set[index] == null) {
      throw new IllegalStateException();
    }
    if (set[index].isOversized()) {
      halveAndPossiblySpread(index);
      return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public void destroy() {
    this.set = (SplayedObject<T>[]) new SplayedObject<?>[0];
  }
}
