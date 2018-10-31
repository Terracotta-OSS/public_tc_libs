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

import java.io.IOException;

/**
 * The interface Cursor. Describes a cursor which can be used to move backward and forward through
 * the tree.
 */
public interface Cursor {

  /**
   * Attempt to reset the cursor to a new tx in place.
   * @param newTx
   * @return true it worked; false if it called first()
   * @throws IOException
   */
  boolean resetInPlace(Tx<?> newTx) throws IOException;

  /**
   * Move before the first entry in the tree.
   *
   * @throws IOException
   */
  Cursor first() throws IOException;

  /**
   * Move before the last entry in the tree.
   * @throws IOException
   */
  Cursor last() throws IOException;

  /**
   * Move before where the specified key would go. The next key will
   * point to the key if it exists in the tree.
   *
   * @param key
   * @return true if the key was found
   * @throws IOException
   */
  boolean moveTo(Object key) throws IOException;

  /**
   * Raw scan to
   * @param start
   * @return
   * @throws IOException
   */
  boolean scanTo(long start) throws IOException;

  /**
   * Did the last move match exactly.
   *
   * @return
   */
  boolean wasLastMoveMatched();

  /**
   * Did the last scan match exactly.
   *
   * @return
   */
  boolean wasLastScanMatched();

  /**
   * Is there a next key in the tree.
   * @return
   */
  boolean hasNext();

  /**
   * Get the next entry in the tree. Moves forward after fetching it.
   *
   * @return
   */
  BtreeEntry next();

  /**
   * Does the tree have a preveious entry.
   * @return
   */
  boolean hasPrevious();

  /**
   * Get the previous entry in the tree. Moves backward after fetching it.
   *
   * @return
   */
  BtreeEntry previous();

  /**
   * Peek at the next entry
   * @return
   */
  BtreeEntry peekNext();

  /**
   * Peek at the previous entry
   * @return
   */
  BtreeEntry peekPrevious();


}
