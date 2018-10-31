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

import java.io.IOException;

/**
 * @author cschanck
 */
public class DuplicateCursor {
  private Cursor baseCursor;
  private DuplicateReadTx tx;

  public DuplicateCursor(Cursor baseCursor, DuplicateReadTx tx) {
    this.baseCursor = baseCursor;
    this.tx = tx;
  }

  public boolean resetInPlace(DuplicateReadTx newTx) throws IOException {
    return baseCursor.resetInPlace(newTx.readTx);
  }

  public FlattenedDuplicateCursor flat() throws IOException {
    return new FlattenedDuplicateCursor(this);
  }

  public DuplicateReadTx getTx() {
    return tx;
  }

  /**
   * Move before the first entry in the tree.
   *
   * @throws IOException
   */
  DuplicateCursor first() throws IOException {
    baseCursor.first();
    return this;
  }

  /**
   * Move before the last entry in the tree.
   *
   * @throws IOException
   */
  DuplicateCursor last() throws IOException {
    baseCursor.last();
    return this;
  }

  /**
   * Move before where the specified key would go. The next key will
   * point to the key if it exists in the tree.
   *
   * @param key
   * @return true if the key was found
   * @throws IOException
   */
  boolean moveTo(Object key) throws IOException {
    return baseCursor.moveTo(key);
  }

  /**
   * Raw scan to
   *
   * @param start
   * @return
   * @throws IOException
   */
  boolean scanTo(long start) throws IOException {
    return baseCursor.scanTo(start);
  }

  /**
   * Did the last move match exactly.
   *
   * @return
   */
  boolean wasLastMoveMatched() {
    return baseCursor.wasLastMoveMatched();
  }

  /**
   * Did the last scan match exactly.
   *
   * @return
   */
  boolean wasLastScanMatched() {
    return baseCursor.wasLastScanMatched();
  }

  /**
   * Is there a next key in the tree.
   *
   * @return
   */
  boolean hasNext() {
    return baseCursor.hasNext();
  }

  /**
   * Get the next entry in the tree. Moves forward after fetching it.
   *
   * @return
   */
  DuplicateEntry next() {
    BtreeEntry ret = baseCursor.next();
    try {
      return tx.renderValue(ret.getKey(), ret.getValue(), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Does the tree have a preveious entry.
   *
   * @return
   */
  boolean hasPrevious() {
    return baseCursor.hasPrevious();
  }

  /**
   * Get the previous entry in the tree. Moves backward after fetching it.
   *
   * @return
   */
  DuplicateEntry previous() {
    BtreeEntry ret = baseCursor.previous();
    try {
      return tx.renderValue(ret.getKey(), ret.getValue(), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Peek at the next entry
   *
   * @return
   */
  DuplicateEntry peekNext() {
    BtreeEntry ret = baseCursor.peekNext();
    try {
      return tx.renderValue(ret.getKey(), ret.getValue(), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Peek at the previous entry
   *
   * @return
   */
  DuplicateEntry peekPrevious() {
    BtreeEntry ret = baseCursor.peekPrevious();
    try {
      return tx.renderValue(ret.getKey(), ret.getValue(), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


}
