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

import com.terracottatech.sovereign.btrees.bplustree.CursorImpl;
import com.terracottatech.sovereign.btrees.bplustree.ReadAccessor;
import com.terracottatech.sovereign.btrees.bplustree.TxCache;
import com.terracottatech.sovereign.btrees.bplustree.model.BPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.Cursor;
import com.terracottatech.sovereign.btrees.bplustree.model.Node;
import com.terracottatech.sovereign.btrees.bplustree.model.ReadTx;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.LinkedList;

/**
 * The read transactions for the btree. Does not lock. Reserves a slot in the
 * access manager. Commit merely releases the slot.
 *
 * @author cschanck
 */
public class AReadTx<W extends TxDecorator> implements ReadTx<W> {
  private final ABPlusTree<W> tree;
  private final TxCache<ANode<W, ?>> txCache;
  private ReadAccessor slot;
  private final W decorator;
  private volatile long workingRootOffset;
  private volatile long workingRevision;
  private boolean begun = false;

  /**
   * Instantiates a new A read tx.
   *
   * @param tree the tree
   * @param cacheSize the cache size
   */
  public AReadTx(ABPlusTree<W> tree, int cacheSize) {
    this.tree = tree;
    this.txCache = new TxCache<>(cacheSize);
    this.decorator = tree.createDecorator();
    begun = false;
  }

  @Override
  public void begin() {
    txBegin();
  }

  private void txBegin() {
    if (!begun) {
      // This is super subtle. You get the current revision of the tree,
      // register this tx as reading this version of the tree. But then you have
      // to check if the tree was stable across this. If a write managed to
      // complete, and then GC, while you were reserving a version of the
      // tree, you could be viewing an invalid version of the tree. We broke
      // this with the moved to list based access tracking; old
      // method worked.
      while (true) {
        SnapShot tmp = tree.getSnapShot();
        workingRevision = tmp.getRevision();
        workingRootOffset = tmp.getRootAddress();
        this.slot = tree.getAccessManager().assign(this);
        if (tree.getSnapShot() == tmp) {
          break;
        }
        tree.getAccessManager().release(slot);
        workingRootOffset = workingRevision = -1L;
      }
      begun = true;
      txCache.clear();
      decorator.postBeginHook();
    }
  }

  private void txEnd() {
    if (begun) {
      begun = false;
      workingRevision = -1L;
      tree.getAccessManager().release(slot);
      this.slot = null;
    }
  }

  @Override
  public Node readNode(long offset) throws IOException {
    ANode<W, ?> n = txCache.getNode(offset);
    if (n == null) {
      n = new AImmutableNode<>(this, offset);
      txCache.cache(n);
    }
    return n;
  }

  @Override
  public void commit() throws IOException {
    decorator.preCommitHook();
    txEnd();
    decorator.postCommitHook();
  }

  @Override
  public Long find(Object key) throws IOException {
    Cursor c = cursor();
    if (c.moveTo(key)) {
      return c.peekNext().getValue();
    }
    return null;
  }

  @Override
  public Long scan(long keyvalue) throws IOException {
    Cursor c = cursor();
    if (c.scanTo(keyvalue)) {
      return c.peekNext().getValue();
    }
    return null;
  }

  @Override
  public Cursor cursor(long startKey) throws IOException {
    txBegin();
    return new CursorImpl(this, startKey);
  }

  @Override
  public Cursor cursor() throws IOException {
    txBegin();
    return new CursorImpl(this).first();
  }

  @Override
  public long size() {
    txBegin();
    return tree.size(this);
  }

  /**
   * Gets assigned slot.
   *
   * @return the assigned slot
   */
  @Override
  public ReadAccessor getAssignedSlot() {
    return slot;
  }

  @Override
  public W getDecorator() {
    return decorator;
  }

  @Override
  public Collection<Node.VerifyError> verify() throws IOException {
    LinkedList<Node.VerifyError> ll = new LinkedList<>();
    tree.verify(this, ll);
    return ll;
  }

  @Override
  public void dump(PrintStream out) throws IOException {
    tree.dump(out);
  }

  @Override
  public void close() {
    txBegin();
    txEnd();
    decorator.preCloseHook();
    workingRootOffset = workingRevision = -1L;
    decorator.postCloseHook();
  }

  /**
   * Gets working revision.
   *
   * @return the working revision
   */
  @Override
  public long getWorkingRevision() {
    return workingRevision;
  }

  /**
   * Gets tree.
   *
   * @return the tree
   */
  @Override
  public BPlusTree<W> getTree() {
    return tree;
  }

  /**
   * Gets working root offset.
   *
   * @return the working root offset
   */
  @Override
  public long getWorkingRootOffset() {
    return workingRootOffset;
  }


  @Override
  public boolean verify(Collection<Node.VerifyError> errors) throws IOException {
    txBegin();
    return getTree().verify(this, errors);
  }

  @Override
  public String toString() {
    return "AReadTx{" + "slot=" + slot + ", workingRootOffset=" + workingRootOffset + ", workingRevision=" + workingRevision + ", begun=" + begun + '}';
  }
}
