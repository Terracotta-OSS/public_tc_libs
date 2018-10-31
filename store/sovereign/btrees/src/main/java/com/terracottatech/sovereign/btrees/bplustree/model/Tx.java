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

import com.terracottatech.sovereign.btrees.bplustree.ReadAccessor;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;

/**
 * The Transaction interface
 */
public interface Tx<W extends TxDecorator> {


  /**
   * The enum Type.
   */
  enum Type {

    READ,

    WRITE
  }


  /**
   * Verify the tree, collect the errors.
   *
   * @param errors
   * @return
   * @throws IOException
   */
  boolean verify(Collection<Node.VerifyError> errors) throws IOException;

  /**
   * Read a node from the store. Use cache if possible.
   *
   * @param offset
   * @return
   * @throws IOException
   */
  Node readNode(long offset) throws IOException;

  /**
   * Closes this tx.
   */
  void close();

  /**
   * Gets working revision.
   *
   * @return the working revision
   */
  long getWorkingRevision();

  /**
   * Gets tree.
   *
   * @return the tree
   */
  BPlusTree<W> getTree();

  /**
   * Gets working root offset.
   *
   * @return the working root offset
   */
  long getWorkingRootOffset();

  /**
   * Explicit begin; done automatically by the operations, here for completeness.
   */
  void begin();

  /**
   * Commit this view of the tree. Next begin() will start a new one.
   *
   * @throws IOException
   */
  void commit() throws IOException;

  /**
   * Find an object in the tree.
   *
   * @param key
   * @return
   * @throws IOException
   */
  Long find(Object key) throws IOException;

  /**
   * Find an explicit 8 byte value in the tree.
   *
   * @param keyvalue
   * @return
   * @throws IOException
   */
  Long scan(long keyvalue) throws IOException;

  /**
   * Get a cursor.
   *
   * @param key If null, starts at the beginning.
   * @return
   * @throws IOException
   */
  Cursor cursor(long key) throws IOException;

  /**
   * Retrieve a cursor pointing at the first location
   * @return
   * @throws IOException
   */
  Cursor cursor() throws IOException;

  /**
   * Get the size of the tree.
   *
   * @return
   */
  long size();

  /**
   * Gets assigned slot.
   *
   * @return the assigned slot
   */
  ReadAccessor getAssignedSlot();

  /**
   * Get the decorator.
   * @return
   */
  W getDecorator();

  Collection<Node.VerifyError> verify() throws IOException;

  void dump(PrintStream out) throws IOException;

}
