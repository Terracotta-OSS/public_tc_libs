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
package com.terracottatech.sovereign.btrees.duplicate;

import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;
import com.terracottatech.sovereign.btrees.bplustree.model.Cursor;
import com.terracottatech.sovereign.btrees.bplustree.model.Tx;
import com.terracottatech.sovereign.btrees.stores.SimpleStore;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author cschanck
 */
public class DuplicateReadTx implements Closeable {

  private DuplicateBPlusTree tree;
  protected Tx<?> readTx;
  protected SimpleStore dupSpace;

  public DuplicateReadTx(DuplicateBPlusTree tree, Tx<?> readTx) {
    this.tree = tree;
    this.readTx = readTx;
    this.dupSpace = tree.getDupSpace();
  }

  public void close() {
    readTx.close();
  }

  /**
   * Gets working revision.
   *
   * @return the working revision
   */
  public long getWorkingRevision() {
    return readTx.getWorkingRevision();
  }

  /**
   * Gets tree.
   *
   * @return the tree
   */
  public DuplicateBPlusTree getTree() {
    return tree;
  }

  /**
   * Explicit begin; done automatically by the operations, here for completeness.
   */
  public void begin() {
    readTx.begin();
  }

  /**
   * Commit this view of the tree. Next begin() will start a new one.
   *
   * @throws java.io.IOException
   */
  public void commit() throws IOException {
    readTx.commit();
  }

  /**
   * Find an object in the tree.
   *
   * @param key
   * @return
   * @throws IOException
   */
  public DuplicateEntry find(Object key) throws IOException {
    Cursor c = readTx.cursor();
    if (c.moveTo(key)) {
      BtreeEntry ret = c.peekNext();
      return renderValue(ret.getKey(), ret.getValue(), true);
    }
    return null;
  }

  boolean isList(long address) {
    return (address < 0);
  }

  DuplicateEntry renderValue(Long key, Long l, boolean readOnly) throws IOException {
    if (l != null) {
      long v = l.longValue();
      if (isList(v)) {
        LongList ret = readList(v & DuplicateBPlusTree.VALID_MASK, readOnly);
        return new DuplicateEntry(key, ret);
      } else {
        return new DuplicateEntry(key, v);
      }
    }
    return new DuplicateEntry(key);
  }

  LongList readList(long addr, boolean readOnly) throws IOException {
    // opportunistic read, 1 page size
    int onepage = dupSpace.getPageSize();
    ByteBuffer buf;
    if (readOnly) {
      buf = dupSpace.readOnly(addr, onepage);
    } else {
      buf = ByteBuffer.allocate(onepage);
      dupSpace.read(addr, buf);
      buf.clear();
    }
    LongList ll = new LongList(buf, onepage);
    if (ll.size() > LongListProxy.listSizeForPageSize(onepage)) {
      // ffs, read it again, full size.
      int toRead = LongListProxy.byteSizeForListCount(ll.size());
      if (readOnly) {
        buf = dupSpace.readOnly(addr, toRead);
      } else {
        buf = ByteBuffer.allocate(toRead);
        dupSpace.read(addr, buf);
        buf.clear();
      }
      ll = new LongList(buf, onepage);
    }
    return ll;
  }

  /**
   * Retrieve a cursor pointing at the first location
   *
   * @return
   * @throws IOException
   */
  public DuplicateCursor cursor() throws IOException {
    return new DuplicateCursor(readTx.cursor(), this);
  }

  /**
   * Get the size of the tree.
   *
   * @return
   */
  public long size() {
    return readTx.size();
  }

}
