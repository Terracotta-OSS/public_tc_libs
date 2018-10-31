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

import com.terracottatech.sovereign.btrees.bplustree.appendonly.ABPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.BPlusTree;
import com.terracottatech.sovereign.btrees.stores.SimpleStore;

import java.io.IOException;

/**
 * Tree that handles duplicate keys. Restricts values to positive long values only. (63 bits)
 *
 * @author cschanck
 */
public class DuplicateBPlusTree {

  public static final long LIST_BIT = 0x8000000000000000l;
  public static final long VALID_MASK = 0x7fffffffffffffffl;
  private final SimpleStore dupSpace;
  private BPlusTree<?> base;

  /**
   * Create a duplicate handling btree. Provide a base tree. Lists of duplicates will be put in
   * the base node storage space for the tree.
   *
   * @param base Base BPlusTree
   */
  public DuplicateBPlusTree(BPlusTree<?> base) {
    this(base, null);
  }

  /**
   * Create a duplicate handling btree. Provide a base tree, and a secondary space (presumable
   * tied to the base tree, better be) to store lists of duplicates. This allows page sizes for the
   * list storage to be controlled seperately.
   *
   * @param base Base BPlusTree
   * @param dupSpace secondary storage space for lists of values. Must be a seconday storage space
   * for base tree. If null, uses the node store fromt he base tree.
   */
  public DuplicateBPlusTree(BPlusTree<?> base, SimpleStore dupSpace) {
    this.base = base;
    this.dupSpace = dupSpace == null ? base.getTreeStore() : dupSpace;
  }

  public BPlusTree<?> getBase() {
    return base;
  }

  public void close() throws IOException {
    base.close();
  }

  public DuplicateReadTx readTx() {
    return readTx(ABPlusTree.DEFAULT_TX_CACHE_SIZE);
  }

  public DuplicateReadTx readTx(int cacheSize) {
    return new DuplicateReadTx(this, base.readTx(cacheSize));
  }

  public DuplicateWriteTx writeTx() {
    return writeTx(ABPlusTree.DEFAULT_TX_CACHE_SIZE);
  }

  public DuplicateWriteTx writeTx(int cacheSize) {
    return new DuplicateWriteTx(this, base.writeTx(cacheSize));
  }

  SimpleStore getDupSpace() {
    return dupSpace;
  }
}
