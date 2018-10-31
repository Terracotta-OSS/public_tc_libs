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
package com.terracottatech.sovereign.btrees.bplustree;

import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;
import com.terracottatech.sovereign.btrees.bplustree.model.Node;

/**
 * Tree location type, holds a node/index pair.
 *
 * @author cschanck
 */
public class TreeLocation {
  private final Node node;
  private int currentIndex;

  /**
   * Instantiates a new Tree location.
   *
   * @param node the node
   * @param index the index
   */
  public TreeLocation(Node node, int index) {
    this.node = node;
    this.currentIndex = index;
  }

  /**
   * Instantiates a new Tree location.
   *
   * @param loc the loc
   */
  public TreeLocation(TreeLocation loc) {
    this(loc.getNode(), loc.getCurrentIndex());
  }

  /**
   * Gets node.
   *
   * @return the node
   */
  public Node getNode() {
    return node;
  }

  public boolean isIndexValid() {
    return currentIndex >= 0;
  }

  /**
   * Gets current index.
   *
   * @return the current index
   */
  public int getCurrentIndex() {
    return currentIndex;
  }

  /**
   * Sets current index.
   *
   * @param currentIndex the current index
   */
  public void setCurrentIndex(int currentIndex) {
    this.currentIndex = currentIndex;
  }

  /**
   * Gets leaf entry.
   *
   * @return the leaf entry
   */
  public BtreeEntry getLeafEntry() {
    if (node.isLeaf()) {
      int index = currentIndex;
      if (index < 0) {
        index = ~index;
      }
      if (index < node.size()) {
        return new BtreeEntry(node.getKey(index), node.getValue(index));
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return "TreeLoc {node=" + node.getOffset() + " index=" + currentIndex + " val=" + getLeafEntry() + "}";
  }

}
