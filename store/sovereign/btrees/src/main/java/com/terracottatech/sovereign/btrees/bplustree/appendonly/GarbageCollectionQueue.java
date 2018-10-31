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
package com.terracottatech.sovereign.btrees.bplustree.appendonly;

import com.terracottatech.sovereign.btrees.stores.SimpleStore;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * Garbage collection queue, executes free ops based on a revision ceiling.
 *
 * @author cschanck
 */
public class GarbageCollectionQueue {

  private final GCTask task;

  /**
   * The interface GC task.
   */
  public static interface GCTask {

    public void gc(FreeOp op) throws IOException;
  }

  private final int minimumQueueSize;

  /**
   * The type Free op.
   *
   * @author cschanck
   */
  public static class FreeOp {

    private final long address;
    private final long revision;
    private final SimpleStore store;

    /**
     * Instantiates a new Free op.
     *
     * @param store the s
     * @param address the addr
     * @param revision the revision
     */
    protected FreeOp(SimpleStore store, long address, long revision) {
      this.store = store;
      this.address = address;
      this.revision = revision;
    }

    /**
     * Gets address.
     *
     * @return the address
     */
    public long getAddress() {
      return address;
    }

    /**
     * Gets store.
     *
     * @return the store
     */
    public SimpleStore getStore() {
      return store;
    }

    /**
     * Gets revision.
     *
     * @return the revision
     */
    public long getRev() {
      return revision;
    }

  }

  private final LinkedList<FreeOp> q = new LinkedList<FreeOp>();

  /**
   * Instantiates a new Garbage collection queue.
   *
   * @param task the task
   * @param minimumQueueSize the minimum queue size
   */
  public GarbageCollectionQueue(GCTask task, int minimumQueueSize) {
    this.task = task;
    this.minimumQueueSize = minimumQueueSize;
  }

  public void queueFree(SimpleStore s, final long revision, long addr) {
    if (addr >= 0) {
      FreeOp fop = new FreeOp(s, addr, revision);
      q.add(fop);
    }
  }

  public void invalidateRevision(long rev) {
    ListIterator<FreeOp> listy = q.listIterator();
    while (listy.hasNext()) {
      if (listy.next().getRev() == rev) {
        listy.remove();
      }
    }
  }

  public boolean isOverflowing() {
    return q.size() > minimumQueueSize;
  }

  public void garbageCollect(long ceiling) throws IOException {
    if (isOverflowing()) {
      for (Iterator<FreeOp> it = q.iterator(); it.hasNext();) {
        FreeOp op = it.next();
        if (op.getRev() < ceiling) {
          task.gc(op);
          it.remove();
        } else {
          break;
        }
      }
    }
  }
}
