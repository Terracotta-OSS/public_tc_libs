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
import java.util.Collection;

/**
 * The Node interface.
 */
public interface Node {

  /**
   * Number of keys in this node.
   * @return
   */
  int size();

  int minSize();

  /**
   * Get a key at a specific index.
   *
   * @param index the index
   * @return the key
   */
  long getKey(int index);

  /**
   * Locate a key within the keys in this node. Note that it will use
   * the tx to get tot he tree's key handler for comparisons.
   *
   * @param target
   * @return
   * @throws IOException
   */
  public int locate(Object target) throws IOException;

  /**
   * Locate a key within the keys in this node. Note that it will use
   * the tx to get tot he tree's key handler for comparisons.
   *
   * @param target
   * @return
   * @throws IOException
   */
  int locate(long target) throws IOException;

  /**
   * Is this node full?
   *
   * @return
   */
  boolean isFull();

  /**
   * Is this node empty.
   *
   * @return
   */
  boolean isEmpty();

  /**
   * Gets pointer at specific index.
   *
   * @param index the index
   * @return the pointer
   */
  long getPointer(int index);

  /**
   * Is this a leaf node?
   * @return
   */
  boolean isLeaf();

  /**
   * Gets a value at a specfic index.
   *
   * @param index the index
   * @return the value
   */
  long getValue(int index);

  /**
   * Gets the revision this node was committed/crated in.
   *
   * @return the revision
   */
  long getRevision();

  /**
   * Gets offset.
   *
   * @return the offset
   */
  long getOffset();

  /**
   * Gets size.
   *
   * @return the size
   */
  int getSize();

  /**
   * Fetch this nodes right sibling.
   *
   * @param parent
   * @return
   * @throws IOException
   */
  Node fetchRightSibling(Node parent) throws IOException;

  /**
   * Fetch this node's left sibling.
   *
   * @param parent
   * @return
   * @throws IOException
   */
  Node fetchLeftSibling(Node parent) throws IOException;

  /**
   * Verify this node and down. Debugging.
   *
   * @param errors
   * @param isRoot
   * @return
   * @throws IOException
   */
  boolean verifyNodeAndImmediateChildren(Collection<VerifyError> errors, boolean isRoot) throws IOException;

  /**
   * The type Verify error.
   *
   * @author cschanck
   */
  public static class VerifyError {
    private final String error;
    private final Node node;

    /**
     * Instantiates a new Verify error.
     *
     * @param node the node
     * @param err the err
     */
    public VerifyError(Node node, String err) {
      this.node = node;
      this.error = err;
    }

    /**
     * Gets error.
     *
     * @return the error
     */
    public String getError() {
      return error;
    }

    /**
     * Gets node.
     *
     * @return the node
     */
    public Node getNode() {
      return node;
    }

    @Override
    public String toString() {
      return "VerifyError{" +
        "error='" + error + '\'' +
        ", node=" + node +
        '}';
    }
  }

}
