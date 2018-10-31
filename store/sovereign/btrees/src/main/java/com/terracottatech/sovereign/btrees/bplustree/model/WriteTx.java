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
import com.terracottatech.sovereign.btrees.stores.SimpleStore;

import java.io.IOException;

/**
 * The interface Write tx.
 */
public interface WriteTx<W extends TxDecorator> extends Tx<W> {

  /**
   * Throw away the current write tx.
   *
   * @throws IOException
   */
  void discard() throws IOException;

  /**
   * Queue a revision for freeing when the specified revision is no longer visible.
   *
   * @param store
   * @param offset
   * @param revision
   * @throws IOException
   */
  void queueFree(SimpleStore store, long offset, long revision) throws IOException;

  /**
   * Free a node's storage for later reclaiming.
   *
   * @param offset
   * @param revision
   * @throws IOException
   */
  void queueFreeNode(long offset, long revision) throws IOException;

  /**
   * Sets working root offset.
   *
   * @param offset the offset
   */
  void setWorkingRootOffset(long offset);

  /**
   * Commit.
   *
   * @param commitType
   * @throws IOException
   */
  void commit(CommitType commitType) throws IOException;

  /**
   * Insert, replacing as needed.
   *
   * @param key
   * @param value
   * @return
   * @throws IOException
   */
  BtreeEntry insert(long key, long value) throws IOException;

  /**
   * Insert only if not present.
   *
   * @param key
   * @param value
   * @return
   * @throws IOException
   */
  BtreeEntry insertIfAbsent(long key, long value) throws IOException;

  /**
   * Replace a key, only if it exists.
   *
   * @param key
   * @param newValue
   * @return
   * @throws IOException
   */
  BtreeEntry replace(long key, long newValue) throws IOException;

  /**
   * Replace key, only if it is present with the specified value.
   *
   * @param key
   * @param expectedValue
   * @param newValue
   * @return
   * @throws IOException
   */
  BtreeEntry replace(long key, long expectedValue, long newValue) throws IOException;

  MutableNode readNode(long offset) throws IOException;

  /**
   * @param isLeaf
   * @return
   */
  MutableNode createNewNode(boolean isLeaf);

  /**
   * Gets size delta.
   *
   * @return the size delta
   */
  long getSizeDelta();

  /**
   * Gets height delta.
   *
   * @return the height delta
   */
  int getHeightDelta();

  /**
   * Used to record a change in prospective size of the tree. Code smell.
   *
   * @param delta
   */
  void recordSizeDelta(long delta);

  /**
   * Record a change in the height of the tree in this tx. Code Smell.
   *
   * @param delta
   */
  void recordHeightDelta(int delta);

  /**
   * Delete an object (key) from the tree.
   *
   * @param keyObject
   * @return
   * @throws IOException
   */
  BtreeEntry delete(long keyObject) throws IOException;

  /**
   * Delete an object (key) from the tree, only if the specified key exists mapped to the specified value.
   *
   * @param keyObject
   * @param valueObject
   * @return
   * @throws IOException
   */
  BtreeEntry delete(long keyObject, long valueObject) throws IOException;

}
