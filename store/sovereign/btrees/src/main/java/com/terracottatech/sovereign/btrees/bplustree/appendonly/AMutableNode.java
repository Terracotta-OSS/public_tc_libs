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

import com.terracottatech.sovereign.btrees.bplustree.model.MutableNode;
import com.terracottatech.sovereign.btrees.bplustree.model.Node;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.btrees.bplustree.model.WriteTx;
import com.terracottatech.sovereign.common.dumbstruct.Accessor;
import com.terracottatech.sovereign.common.dumbstruct.buffers.SingleDataByteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author cdennis
 */
public class AMutableNode<W extends TxDecorator> extends ANode<W, WriteTx<W>> implements MutableNode {

  private final ByteBuffer buf;
  private final Accessor accessor;
  private final ANodeProxy storageProxy;
  private final long revision;
  private final boolean isLeaf;

  private long offset = -1L;
  private boolean dirty = false;
  private int size = 0;

  /**
   * Instantiates a new node. Will be mutable, marked dirty initially.
   *
   * @param tx the tx
   * @param isLeaf the is leaf
   */
  AMutableNode(WriteTx<W> tx, boolean isLeaf) {
    super(tx);
    this.size = 0;
    this.isLeaf = isLeaf;
    dirty = true;
    this.revision = tx.getWorkingRevision();
    this.buf = ByteBuffer.allocate(sizeFor(tx.getTree().getMaxKeysPerNode()));
    SingleDataByteBuffer dataBuffer = new SingleDataByteBuffer(buf);
    this.accessor = new Accessor(dataBuffer);
    storageProxy = isLeaf ? ANodeProxy.leaf() : ANodeProxy.branch();
    if (!isLeaf) {
      setPointer(0, -1L);
    }
  }

  /**
   * Instantiates a new node, reading from a specific location. If it is a read transaction, then
   * this might become immutable. This is an opt for read heavy cases.
   *
   * @param tx the tx
   * @param offset the offset
   * @throws IOException the iO exception
   */
  AMutableNode(WriteTx<W> tx, long offset) throws IOException {
    super(tx);
    int maxBytesPerNode = tx.getTree().getMaxBytesPerNode();
    this.offset = offset;
    this.buf = ByteBuffer.allocate(maxBytesPerNode);
    SingleDataByteBuffer dataBuffer = new SingleDataByteBuffer(buf);
    this.accessor = new Accessor(dataBuffer);
    tx.getTree().getTreeStore().read(offset, buf);
    buf.clear();
    storageProxy = ANodeProxy.storageFor(accessor);
    this.size = storageProxy.getSize(accessor);
    this.isLeaf = storageProxy.isLeaf();
    this.revision = storageProxy.getRevision(accessor);
  }

  public boolean isDirty() {
    return dirty;
  }

  private void markDirty() {
    if (!dirty) {
      dirty = true;
    }
  }

  /**
   * Number of keys.
   *
   * @return
   */
  @Override
  public int size() {
    return size;
  }

  /**
   * Gets key.
   *
   * @param index the index
   * @return the key
   */
  @Override
  public long getKey(int index) {
    return storageProxy.getKey(accessor, index);
  }

  /**
   * Sets key.
   *
   * @param index the index
   * @param val the val
   */
  @Override
  public void setKey(int index, long val) {
    markDirty();
    storageProxy.setKey(accessor, index, val);
    size = Math.max(size, index + 1);
  }

  private int checkFullSize() {
    if (CHECK_USAGE) {
      if (isFull()) {
        throw new IllegalStateException("Can't add to a full node");
      }
    }
    return size();
  }

  private int checkSizeWithIndex(int index) {
    int sz = checkFullSize();
    if (CHECK_USAGE) {
      if (index > sz) {
        throw new IllegalArgumentException();
      }
    }
    return sz;
  }

  private int sizeFor(int keycount) {
    return sizeFor(isLeaf(), keycount);
  }

  /**
   * Get a pointer. 0 to size() indexed.
   *
   * @param index the index
   * @return the pointer
   */
  @Override
  public long getPointer(int index) {
    assertBranch();
    return storageProxy.getPointer(accessor, index);
  }

  /**
   * Set a pointer.
   *
   * @param index the index
   * @param val the val
   */
  @Override
  public void setPointer(int index, long val) {
    assertBranch();
    markDirty();
    storageProxy.setPointer(accessor, index, val);
  }

  @Override
  public void insertPointerKeyAt(int index, long pointer, long key) {
    assertBranch();
    int sz = checkSizeWithIndex(index);
    markDirty();
    storageProxy.insertPointerKeyAt(accessor, index, pointer, key, size);
    size = sz + 1;
  }

  @Override
  public void insertKeyPointerAt(int index, long key, long pointer) {
    assertBranch();
    int sz = checkSizeWithIndex(index);
    markDirty();
    storageProxy.insertKeyPointerAt(accessor, index, key, pointer, size);
    size = sz + 1;
  }

  @Override
  public void insertKeyValueAt(int index, long key, long value) {
    assertLeaf();
    markDirty();
    int sz = checkSizeWithIndex(index);
    storageProxy.insertKeyValueAt(accessor, index, key, value, sz);
    size = sz + 1;
  }

  private void assertLeaf() {
    if (CHECK_USAGE) {
      if (!isLeaf()) {
        throw new AssertionError("Unsupported branch operation");
      }
    }
  }

  private void assertBranch() {
    if (CHECK_USAGE) {
      if (isLeaf()) {
        throw new AssertionError("Unsupported leaf operation");
      }
    }
  }

  @Override
  public boolean isLeaf() {
    return isLeaf;
  }

  /**
   * Gets value.
   *
   * @param index the index
   * @return the value
   */
  @Override
  public long getValue(int index) {
    assertLeaf();
    return storageProxy.getValue(accessor, index);
  }

  /**
   * Sets value.
   *
   * @param index the index
   * @param val the val
   */
  @Override
  public void setValue(int index, long val) {
    assertLeaf();
    markDirty();
    storageProxy.setValue(accessor, index, val);
  }

  @Override
  public long flush() throws IOException {
    if (dirty) {
      if (getOffset() >= 0) {
        tx.queueFreeNode(getOffset(), revision);
      }
      buf.clear();
      storageProxy.setHeader(accessor, size);
      storageProxy.setRevision(accessor, tx.getWorkingRevision());
      buf.limit(sizeFor(size()));
      offset = tx.getTree().getTreeStore().append(buf);
      buf.clear();
      dirty = false;
    }

    return offset;
  }

  /**
   * Gets revision.
   *
   * @return the revision
   */
  @Override
  public long getRevision() {
    return revision;
  }

  /**
   * Gets offset.
   *
   * @return the offset
   */
  @Override
  public long getOffset() {
    return offset;
  }

  /**
   * Sets size.
   *
   * @param size the size
   */
  @Override
  public void setSize(int size) {
    this.size = size;
    dirty = true;
  }

  /**
   * Gets size.
   *
   * @return the size
   */
  @Override
  public int getSize() {
    return size;
  }

  @Override
  public void removeKeyValueAt(int point) {
    assertLeaf();
    markDirty();
    storageProxy.removeKeyValueAt(accessor, point, size);
    size--;
  }

  @Override
  public void removeKeyPointerAt(int point) {
    assertBranch();
    markDirty();
    storageProxy.removeKeyPointerAt(accessor, point, size);
    size--;
  }

  @Override
  public void removePointerKeyAt(int point) {
    assertBranch();
    markDirty();
    storageProxy.removePointerKeyAt(accessor, point, size);
    size--;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SplitReturn split() throws IOException {
    int splitPoint = size() / 2;
    long splitKey = getKey(splitPoint);
    AMutableNode<W> newNode;

    if (isLeaf()) {
      newNode = (AMutableNode<W>) tx.createNewNode(true);
      storageProxy.splitCopy(splitPoint, accessor, newNode.accessor, size);
      newNode.setSize(size() - splitPoint);
      setSize(splitPoint);
    } else {
      newNode = (AMutableNode<W>) tx.createNewNode(false);
      storageProxy.splitCopy(splitPoint, accessor, newNode.accessor, size);
      newNode.setSize(size() - splitPoint - 1);
      setSize(splitPoint);
    }

    flush();
    newNode.flush();
    return new SplitReturn(newNode, splitKey);
  }

  /**
   * This is critically important for rebalancing; find the smallest key under this node.
   * In the way in which this is called, the read should be super cheap because the
   * tx cache should be hit all the time.
   *
   * @return
   * @throws IOException
   */
  @Override
  public long smallestKeyUnder() throws IOException {
    if (isLeaf()) {
      return getKey(0);
    } else {
      return tx.readNode(getPointer(0)).smallestKeyUnder();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public AMutableNode<W> fetchLeftSibling(Node parent) throws IOException {
    return (AMutableNode<W>) super.fetchLeftSibling(
      parent); //To change body of generated methods, choose Tools | Templates.
  }

  @SuppressWarnings("unchecked")
  @Override
  public AMutableNode<W> fetchRightSibling(Node parent) throws IOException {
    return (AMutableNode<W>) super.fetchRightSibling(
      parent); //To change body of generated methods, choose Tools | Templates.
  }

}
