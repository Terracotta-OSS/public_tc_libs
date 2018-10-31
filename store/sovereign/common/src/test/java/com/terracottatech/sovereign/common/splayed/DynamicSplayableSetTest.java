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

import com.terracottatech.sovereign.common.utils.MiscUtils;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.assertTrue;

/**
 * Created by cschanck on 2/29/2016.
 */
public class DynamicSplayableSetTest {

  static class HalvableSet implements DynamicSplayableSet.SplayedObject<HalvableSet> {
    private HashSet<Integer> val;

    public HalvableSet() {
      this.val = new HashSet<>();
    }

    public HalvableSet(HashSet<Integer> val) {
      this.val = val;
    }

    @Override
    public HalvableSet object() {
      return this;
    }

    @Override
    public HalvableSet halve(DynamicSplayableSet.HashSpreadFunc hash, int bitpos) {
      HashSet<Integer> newbie = new HashSet<Integer>();
      int mask = 1 << bitpos;
      for (Integer ii : val) {
        int h = hash.hash(ii.hashCode());
        h = h & mask;
        if (h == 0) {
          newbie.add(ii);
        }
      }
      val.removeAll(newbie);
      return new HalvableSet(newbie);
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean isOversized() {
      return false;
    }

    public HashSet<Integer> getVal() {
      return val;
    }

    @Override
    public String toString() {
      return val.toString();
    }
  }

  @Test
  public void testSpreading() throws Exception {
    DynamicSplayableSet<HalvableSet> set = new DynamicSplayableSet<HalvableSet>((i) -> {
      return MiscUtils.hash32shiftmult(i);
    }, new HalvableSet());

    for (int i = 0; i < 10000; i = i + 100) {
      set.shardAt(0).getVal().add(i);
    }
    System.out.println(set);
    for(HalvableSet hs:set.shards()) {
      System.out.println(hs.getVal());
    }
    set.halveAndPossiblySpread(0);
    System.out.println(set);
    verify(set,0,10000,100);

    set.halveAndPossiblySpread(1);
    System.out.println(set);
    verify(set,0,10000,100);

    set.halveAndPossiblySpread(3);
    System.out.println(set);
    verify(set,0,10000,100);

    set.halveAndPossiblySpread(set.set.length - 1);
    System.out.println(set);
    verify(set,0,10000,100);

    set.halveAndPossiblySpread(7);
    System.out.println(set);
    verify(set,0,10000,100);

    set.halveAndPossiblySpread(7);
    System.out.println(set);
    verify(set,0,10000,100);

    set.halveAndPossiblySpread(3);
    System.out.println(set);
    verify(set,0,10000,100);

    set.halveAndPossiblySpread(1);
    System.out.println(set);
    verify(set,0,10000,100);

    set.halveAndPossiblySpread(0);
    System.out.println(set);
    verify(set,0,10000,100);

  }

  private void verify(DynamicSplayableSet<HalvableSet> set, int from, int to, int skip) {
    for(int i=from;i<to;i=i+skip) {
      assertTrue(set.shardAt(set.shardIndexFor(i)).getVal().contains(i));
    }
  }
}
