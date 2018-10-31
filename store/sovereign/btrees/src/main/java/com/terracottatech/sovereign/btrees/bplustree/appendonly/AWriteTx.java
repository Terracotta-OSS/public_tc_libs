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

import com.terracottatech.sovereign.btrees.bplustree.CommitType;
import com.terracottatech.sovereign.btrees.bplustree.CursorImpl;
import com.terracottatech.sovereign.btrees.bplustree.ReadAccessor;
import com.terracottatech.sovereign.btrees.bplustree.TxCache;
import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;
import com.terracottatech.sovereign.btrees.bplustree.model.Cursor;
import com.terracottatech.sovereign.btrees.bplustree.model.Node;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.btrees.bplustree.model.WriteTx;
import com.terracottatech.sovereign.btrees.stores.SimpleStore;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The write transaction. First operation locks the tree for writes, the lock is held
 * until a commit/discard is done.
 *
 * @author cschanck
 */
public class AWriteTx<W extends TxDecorator> implements WriteTx<W> {

  private final ABPlusTree<W> tree;
  private final TxCache<AMutableNode<W>> txCache;
  private final ReadAccessor slot;
  private final W decorator;
  private long workingRootOffset;
  private volatile long workingRevision = -1L;
  private boolean dirty = false;
  private boolean begun = false;
  private long sizeDelta;
  private final ReentrantLock lock;
  private int heightDelta;

  /**
   * Instantiates a new A write tx.
   *
   * @param tree the tree
   * @param cacheSize the cache size
   */
  public AWriteTx(ABPlusTree<W> tree, int cacheSize) {
    this.tree = tree;
    this.txCache = new TxCache<>(cacheSize);
    this.lock = tree.getTreeLock();
    this.decorator = tree.createDecorator();
    slot = tree.getAccessManager().assign(this);
  }

  private void lockPlease() {
    lock.lock();
  }

  @Override
  public void begin() {
    if (!begun) {
      lockPlease();
      workingRootOffset = tree.getSnapShot().getRootAddress();
      workingRevision = tree.getAccessManager().nextRevisionNumber();
      txCache.clear();
      dirty = false;
      begun = true;
      sizeDelta = 0L;
      decorator.postBeginHook();
    }
  }

  private void beginDirty() {
    begin();
    dirty = true;
  }

  /**
   * Gets tree.
   *
   * @return the tree
   */
  @Override
  public ABPlusTree<W> getTree() {
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
  public void queueFree(SimpleStore s, long offset, long revision) throws IOException {
    if (getWorkingRevision() == revision) {
      s.storeFree(offset, true);
    } else {
      getTree().queueFree(s, getWorkingRevision(), offset);
    }
  }

  @Override
  public void queueFreeNode(long offset, long revision) throws IOException {
    txCache.invalidate(offset);
    queueFree(getTree().getTreeStore(), offset, revision);
  }

  /**
   * Sets working root offset.
   *
   * @param offset the offset
   */
  @Override
  public void setWorkingRootOffset(long offset) {
    workingRootOffset = offset;
    dirty = true;
  }

  @Override
  public BtreeEntry insert(long key, long value) throws IOException {
    beginDirty();
    return tree.insert(this, key, value);
  }

  @Override
  public BtreeEntry insertIfAbsent(long key, long value) throws IOException {
    beginDirty();
    return tree.insertIfAbsent(this, key, value);
  }

  @Override
  public BtreeEntry replace(long key, long newValue) throws IOException {
    beginDirty();
    return tree.replace(this, key, newValue);
  }

  @Override
  public BtreeEntry replace(long key, long expectedValue, long newValue) throws IOException {
    beginDirty();
    return tree.replace(this, key, expectedValue, newValue);
  }

  @Override
  public void recordSizeDelta(long delta) {
    sizeDelta = sizeDelta + delta;
  }

  @Override
  public void recordHeightDelta(int delta) {
    heightDelta = heightDelta + delta;
  }

  /**
   * Gets size delta.
   *
   * @return the size delta
   */
  @Override
  public long getSizeDelta() {
    return sizeDelta;
  }

  /**
   * Gets height delta.
   *
   * @return the height delta
   */
  @Override
  public int getHeightDelta() {
    return heightDelta;
  }

  @Override
  public BtreeEntry delete(long keyObject, long valueObject) throws IOException {
    beginDirty();
    return tree.delete(this, keyObject, valueObject);
  }

  @Override
  public BtreeEntry delete(long key) throws IOException {
    beginDirty();
    return tree.delete(this, key);
  }

  @Override
  public void discard() throws IOException {
    if (begun) {
      Throwable tt = null;

      try {
        tree.getTreeStore().discard();
      } catch (Throwable t) {
        tt = t;
      }

      try {
        tree.invalidateRevision(workingRevision);
      } catch (Throwable t) {
        if (tt == null) {
          tt = t;
        } else {
          tt.addSuppressed(t);
        }
      }
      try {
        tree.garbageCollect();
      } catch (Throwable t) {
        if (tt == null) {
          tt = t;
        } else {
          tt.addSuppressed(t);
        }
      }

      workingRevision = -1;
      begun = false;
      sizeDelta = 0L;
      dirty = false;
      lock.unlock();
      tree.getStats().recordDiscard();
      if (tt != null) {
        throw new IOException(tt);
      }
    }
  }

  @Override
  public void commit(CommitType commitType) throws IOException {
    if (begun || commitType.equals(CommitType.DURABLE) || tree.isDurableCommitPending()) {
      begin();
      Throwable tt = null;

      try {
        decorator.preCommitHook();
      } catch (Throwable t) {
        tt = t;
      }

      try {
        tree.commit(this, commitType);
      } catch (Throwable t) {
        if (tt == null) {
          tt = t;
        } else {
          tt.addSuppressed(t);
        }
      }

      try {
        tree.garbageCollect();
      } catch (Throwable t) {
        if (tt == null) {
          tt = t;
        } else {
          tt.addSuppressed(t);
        }
      }

      workingRevision = -1;
      begun = false;
      dirty = false;
      sizeDelta = 0;
      lock.unlock();
      decorator.postCommitHook();
      if (tt != null) {
        throw new IOException(tt);
      }
    }

  }

  @Override
  public void commit() throws IOException {
    commit(CommitType.VISIBLE);
  }

  @Override
  public void close() {
    if (dirty) {
      throw new IllegalStateException("Unable to close inflight transaction!");
    }
    if (begun) {
      workingRevision = -1;
      begun = false;
      lock.unlock();
    }
    decorator.preCloseHook();
    tree.getAccessManager().release(slot);
    workingRevision = -1;
    workingRootOffset = -1L;
    txCache.clear();
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
    begin();
    return new CursorImpl(this, startKey);
  }

  @Override
  public Cursor cursor() throws IOException {
    return new CursorImpl(this).first();
  }

  @Override
  public long size() {
    begin();
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
  public AMutableNode<W> readNode(long offset) throws IOException {
    AMutableNode<W> n = txCache.getNode(offset);
    if (n == null) {
      n = new AMutableNode<>(this, offset);
      txCache.cache(n);
    }
    return n;
  }

  @Override
  public AMutableNode<W> createNewNode(boolean isLeaf) {
    return new AMutableNode<>(this, isLeaf);
  }

  @Override
  public boolean verify(Collection<Node.VerifyError> errors) throws IOException {
    begin();
    return getTree().verify(this, errors);
  }
}
