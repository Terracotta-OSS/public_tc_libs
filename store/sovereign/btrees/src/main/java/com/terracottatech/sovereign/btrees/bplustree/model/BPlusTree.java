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
package com.terracottatech.sovereign.btrees.bplustree.model;

import com.terracottatech.sovereign.btrees.bplustree.CommitType;
import com.terracottatech.sovereign.btrees.bplustree.Finger;
import com.terracottatech.sovereign.btrees.bplustree.appendonly.SnapShot;
import com.terracottatech.sovereign.btrees.stores.SimpleStore;

import java.io.IOException;
import java.util.Collection;

/**
 * The interface for a B+Tree.
 *
 */
public interface BPlusTree<W extends TxDecorator> {

  KeyValueHandler<TxDecorator> DEFAULT_LONG_HANDLER = new KeyValueHandler<TxDecorator>() {

    @Override
    public long compareLongKeys(Tx<TxDecorator> tx, long target, long k2) {
      return Long.compare(target, k2);
    }

    @Override
    public long compareObjectKeys(Tx<TxDecorator> tx, Object target, long k2) {
      return Long.compare((Long) target, k2);
    }
  };

  /**
   * Gets tree store underlying this tree.
   *
   * @return the tree store
   */
  SimpleStore getTreeStore();

  /**
   * Gets max keys per node.
   *
   * @return the max keys per node
   */
  int getMaxKeysPerNode();

  /**
   * Gets max bytes per node.
   *
   * @return the max bytes per node
   */
  int getMaxBytesPerNode();

  /**
   * Gets comparator for this tree.
   *
   * @return the comparator
   */
  KeyValueHandler<W> getComparator();

  /**
   * Commit the state of the tree, either visibly or durably.
   * @param treeTx
   * @param commitType
   * @throws IOException
   */
  void commit(WriteTx<W> treeTx, CommitType commitType) throws IOException;

  /*
   * get the size of the tree.
   */
  long size(Tx<W> treeTx);

  /**
   * get the height of the tree.
   * @param treeTx
   * @return
   */
  int height(Tx<W> treeTx);

  /**
   * Insert into the tree.
   *
   * @param tx
   * @param key
   * @param value
   * @return
   * @throws IOException
   */
  BtreeEntry insert(WriteTx<W> tx, long key, long value) throws IOException;

  /**
   * Insert only if absent.
   * @param tx
   * @param key
   * @param value
   * @return
   * @throws IOException
   */
  BtreeEntry insertIfAbsent(WriteTx<W> tx, long key, long value) throws IOException;

  /**
   * Replace, only if the key exists in the tree.
   *
   * @param tx
   * @param key
   * @param newValue
   * @return
   * @throws IOException
   */
  BtreeEntry replace(WriteTx<W> tx, long key, long newValue) throws IOException;

  /**
   * Replace, only if key exists and has specified value.
   *
   * @param tx
   * @param key
   * @param expectedValue
   * @param newValue
   * @return
   * @throws IOException
   */
  BtreeEntry replace(WriteTx<W> tx, long key, long expectedValue, long newValue) throws IOException;

  /**
   * Close the tree.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Get a read transaction withe the specified tx cache size.
   *
   * @param cacheSize
   * @return
   */
  ReadTx<W> readTx(int cacheSize);

  /**
   * Get a write transaction with the specified tx cache size.
   *
   * @param cacheSize
   * @return
   */
  WriteTx<W> writeTx(int cacheSize);

  /**
   * Verify the tree has a coherent structure. Debugging.
   *
   * @param tx
   * @param errors
   * @return
   * @throws IOException
   */
  public boolean verify(Tx<W> tx, Collection<Node.VerifyError> errors) throws IOException;

  /**
   * Search returning a TreeLocation object, used for cursors.
   *
   * @param tx
   * @param key
   * @return
   * @throws IOException
   */
  public Finger searchForNode(Tx<W> tx, Object key) throws IOException;

  /**
   * Search, using a key value directly.
   * @param tx
   * @param start
   * @return
   */
  Finger scanForNode(Tx<W> tx, long start) throws IOException;

  /**
   * Gets snap shot. MVCC view into the database. Used by transactions.
   *
   * @return the snap shot
   */
  public SnapShot getSnapShot();

  /**
   * Delete a key from the tree.
   *
   * @param tx
   * @param key
   * @return
   * @throws IOException
   */
  BtreeEntry delete(WriteTx<W> tx, long key) throws IOException;

  /**
   * Delete a key from the tree oly if it has the specified value.
   *
   * @param tx
   * @param key
   * @param value
   * @return
   * @throws IOException
   */
  BtreeEntry delete(WriteTx<W> tx, long key, long value) throws IOException;

  Stats getStats();

  void notifyOfGC(long revision);

  IngestHandle startBatch();

  IngestHandle startBatch(WriteTx<W> wt);
}
