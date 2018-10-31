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

import com.terracottatech.sovereign.btrees.bplustree.model.Tx;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.common.dumbstruct.Accessor;
import com.terracottatech.sovereign.common.dumbstruct.buffers.SingleDataByteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author cdennis
 */
public class AImmutableNode<W extends TxDecorator> extends ANode<W, Tx<W>> {
  public static final boolean READ_ONLY = true;

  private final ByteBuffer buf;
  private final Accessor accessor;
  private final ANodeProxy storageProxy;
  private final long revision;
  private final boolean isLeaf;
  private final long offset;
  private final int size;

  /**
   * Instantiates a new node, reading from a specific location. If it is a read transaction, then
   * this might become immutable. This is an opt for read heavy cases.
   *
   * @param tx the tx
   * @param offset the offset
   * @throws IOException the iO exception
   */
  AImmutableNode(Tx<W> tx, long offset) throws IOException {
    super(tx);
    this.offset = offset;
    int maxBytesPerNode = tx.getTree().getMaxBytesPerNode();
    if (READ_ONLY && tx.getTree().getTreeStore().supportsReadOnly()) {
      this.buf = tx.getTree().getTreeStore().readOnly(offset, maxBytesPerNode);
    } else {
      this.buf = ByteBuffer.allocate(maxBytesPerNode);
      tx.getTree().getTreeStore().read(offset, buf);
    }
    buf.clear();
    this.accessor = new Accessor(new SingleDataByteBuffer(buf));
    this.storageProxy = ANodeProxy.storageFor(accessor);
    this.size = storageProxy.getSize(accessor);
    this.isLeaf = storageProxy.isLeaf();
    revision = storageProxy.getRevision(accessor);
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
   * Gets size.
   *
   * @return the size
   */
  @Override
  public int getSize() {
    return size;
  }
}
