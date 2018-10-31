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
package com.terracottatech.sovereign.btrees.bplustree;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicInteger;

public class StripedListBasedAccessTrackerImpl implements ListBasedAccessTracker {
  private final ListBasedAccessTracker[] stripes;
  private final int mask;
  private AtomicInteger roundRobin = new AtomicInteger(0);

  public StripedListBasedAccessTrackerImpl() {
    this(Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
  }

  public StripedListBasedAccessTrackerImpl(int max) {
    max = (int) Math.max(1L, Long.highestOneBit(max - 1) << 1);
    stripes = new ListBasedAccessTracker[max];
    for (int i = 0; i < max; i++) {
      stripes[i] = new SingleListBasedAccessTrackerImpl();
    }
    this.mask = stripes.length - 1;
  }

  private int nextStripe() {
    int stripe = roundRobin.incrementAndGet() & this.mask;
    return stripe;
  }

  @Override
  public void deregisterRead(ReadAccessor accessor) {
    stripes[accessor.getStripe()].deregisterRead(accessor);
  }

  @Override
  public ReadAccessor registerRead(Object obj, long revision) {
    int stripe = nextStripe();
    ReadAccessor ret = stripes[stripe].registerRead(obj, revision);
    ret.stripe(stripe);
    return ret;
  }

  @Override
  public long getLowestActiveRevision(long ceiling) {
    long ret = getLowestActiveRevision();
    if (ret < 0 || ret >= ceiling) {
      return ceiling;
    }
    return ret;
  }

  @Override
  public long getLowestActiveRevision() {
    long ret = Long.MAX_VALUE;
    for (int i = 0; i < stripes.length; i++) {
      long p = stripes[i].getLowestActiveRevision();
      if (p >= 0) {
        ret = Math.min(p, ret);
      }
    }
    if (ret == Long.MAX_VALUE) {
      return -1L;
    }
    return ret;
  }

  @Override
  public String dumpActive() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    boolean first = true;
    for (int i = 0; i < stripes.length; i++) {
      String p = stripes[i].dumpActive();
      if (p.length() > 0) {
        if (first) {
          first = false;
        } else {
          pw.println();
        }
        pw.print(p);
      }
    }
    pw.flush();
    return sw.toString();
  }

}
