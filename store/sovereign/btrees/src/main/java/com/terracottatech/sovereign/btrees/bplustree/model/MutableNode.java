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

import com.terracottatech.sovereign.btrees.bplustree.appendonly.SplitReturn;
import java.io.IOException;

/**
 *
 * @author cdennis
 */
public interface MutableNode extends Node {

  /**
   * Sets size.
   *
   * @param size the size
   */
  void setSize(int size);

  /**
   * Sets key at a specific index. Replaces key value if necessary.
   *
   * @param index the index
   * @param val the val
   */
  void setKey(int index, long val);

  /**
   * Sets pointer.
   *
   * @param index the index
   * @param val the val
   */
  void setPointer(int index, long val);

  /**
   * insert a point/key pair at the specified index. Makes room.
   *
   * @param index
   * @param pointer
   * @param key
   */
  void insertPointerKeyAt(int index, long pointer, long key);

  /**
   * Inserts a key/pointer pair at the specified index. Makes room.
   *
   * @param index
   * @param pointer
   * @param key
   */
  void insertKeyPointerAt(int index, long pointer, long key);

  /**
   * Inserts a key/value pair at the specified index. Makes room.
   *
   * @param index
   * @param key
   * @param value
   */
  void insertKeyValueAt(int index, long key, long value);

  /**
   * Sets a value.
   *
   * @param index the index
   * @param val the val
   */
  void setValue(int index, long val);

  /**
   * Flush's the current contents of the node to the store.
   *
   * @return
   * @throws IOException
   */
  long flush() throws IOException;

  /**
   * Split this node.
   *
   * @return
   * @throws IOException
   */
  SplitReturn split() throws IOException;

  /**
   * Remove a key/value pair at a key index.
   *
   * @param point
   */
  void removeKeyValueAt(int point);

  /**
   * Remove a key/pointer pair at a key index.
   *
   * @param i
   */
  void removeKeyPointerAt(int i);

  /**
   * Remove a pointer/key pair at a pointer index.
   *
   * @param i
   */
  void removePointerKeyAt(int i);

  /**
   * Find the lowest key under this node. Useful for delete processing.
   *
   * @return
   * @throws IOException
   */
  long smallestKeyUnder() throws IOException;

  @Override
  public MutableNode fetchLeftSibling(Node parent) throws IOException;

  @Override
  public MutableNode fetchRightSibling(Node parent) throws IOException;


}
