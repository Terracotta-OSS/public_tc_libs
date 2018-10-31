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
package com.terracottatech.sovereign.btrees.bplustree.appendonly;

import com.terracottatech.sovereign.btrees.bplustree.model.Node;
import com.terracottatech.sovereign.btrees.bplustree.model.Tx;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;

/**
 * The node implementation for the B+Tree. Both a leaf and a branch.
 * Uses the {@link com.terracottatech.sovereign.btrees.bplustree.appendonly.ANodeProxy}
 * proxy class to actually manipulate the data.
 * <p>The data is laid out conceptually as a leaf/branch flag, a size, a mutation revision,
 * and then N many keys (0-based) and N-many values (leaf), or N+1 many pointers and N-many keys.</p>
 *
 * @author cschanck
 */
public abstract class ANode<W extends TxDecorator, T extends Tx<W>> implements Node {
  public static final boolean CHECK_USAGE = false;

  protected final T tx;

  /**
   * Instantiates a new node, reading from a specific location. If it is a read transaction, then
   * this might become immutable. This is an opt for read heavy cases.
   *
   * @param tx the tx
   * @throws IOException the iO exception
   */
  ANode(T tx) {
    this.tx = tx;
  }

  public static int sizeFor(boolean leaf, int keyCount) {
    if (leaf) {
      return ANodeProxy.leaf().sizeFor(keyCount);
    } else {
      return ANodeProxy.branch().sizeFor(keyCount);
    }
  }

  public static int maxSizeFor(int keycount) {
    return Math.max(sizeFor(false, keycount), sizeFor(true, keycount));
  }

  /**
   * Locate based on long.
   *
   * @param target
   * @return
   * @throws IOException
   */
  @Override
  public int locate(long target) throws IOException {
    int min = 0;
    int max = size() - 1;
    while (max >= min) {
      int mid = min + ((max - min) / 2);
      long cmp = tx.getTree().getComparator().compareLongKeys(tx, target, getKey(mid));
      if (cmp == 0) {
        return mid;
      } else if (cmp > 0L) {
        min = mid + 1;
      } else {
        max = mid - 1;
      }
    }
    return ~min;
  }

  /**
   * Locate based on object.
   *
   * @param target
   * @return
   * @throws IOException
   */
  @Override
  public int locate(Object target) throws IOException {
    int min = 0;
    int max = size() - 1;
    while (max >= min) {
      int mid = min + ((max - min) / 2);
      long cmp = tx.getTree().getComparator().compareObjectKeys(tx, target, getKey(mid));
      if (cmp == 0) {
        return mid;
      } else if (cmp > 0L) {
        min = mid + 1;
      } else {
        max = mid - 1;
      }
    }

    return ~min;
  }

  @Override
  public boolean isFull() {
    return size() >= tx.getTree().getMaxKeysPerNode();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public String toString() {
    return toString(true);
  }

  public String toString(boolean includeLeafValues) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.print("Node@" + getOffset() + " " + size() + "/" + getRevision() + "-" + (isLeaf() ? "L" : "B"));
    pw.print(" Keys [");
    for (int i = 0; i < size(); i++) {
      pw.print(getKey(i) + ", ");
    }
    pw.print("] ");
    if (isLeaf()) {
      if (includeLeafValues) {
        pw.print("Vals [");
        for (int i = 0; i < size(); i++) {
          pw.print(getValue(i) + ", ");
        }
        pw.print("]");
      }
    } else {
      pw.print("Pointers [");
      for (int i = 0; i < size() + 1; i++) {
        pw.print(getPointer(i) + ", ");
      }
      pw.print("]");
    }
    pw.close();
    return sw.toString();
  }

  public int minSize() {
    return tx.getTree().getMaxKeysPerNode() / 2 - 1;
  }

  /**
   * Find the right sibling of this node.
   *
   * @param parent
   * @return
   * @throws IOException
   */
  @Override
  public Node fetchRightSibling(Node parent) throws IOException {
    if (parent != null) {
      int positionInParent = parent.locate(this.getKey(0));
      if (positionInParent < 0) {
        positionInParent = ~positionInParent;
      } else {
        positionInParent++;
      }
      int nextPointer = positionInParent + 1;
      if (nextPointer > parent.size()) {
        return null;
      }
      long pos = parent.getPointer(nextPointer);
      return tx.readNode(pos);
    }
    return null;
  }

  /**
   * Find the left sibling of this node.
   *
   * @param parent
   * @return
   * @throws IOException
   */
  @Override
  public Node fetchLeftSibling(Node parent) throws IOException {
    if (parent != null) {
      int positionInParent = parent.locate(this.getKey(0));
      if (positionInParent < 0) {
        positionInParent = ~positionInParent;
      } else {
        positionInParent++;
      }
      int nextPointer = positionInParent - 1;
      if (nextPointer < 0) {
        return null;
      }
      long pos = parent.getPointer(nextPointer);
      return tx.readNode(pos);
    }
    return null;
  }

  @Override
  public boolean verifyNodeAndImmediateChildren(Collection<VerifyError> errors, boolean isRoot) throws IOException {
    boolean ok = true;

    if (getOffset() != tx.getWorkingRootOffset()) {
      if (size() >= tx.getTree().getMaxKeysPerNode()) {
        ok = false;
        errors.add(new VerifyError(this, "Node over flowing."));
      }
      if (!isRoot && size() < minSize()) {
        ok = false;
        errors.add(new VerifyError(this, "Node under flowing."));
      }
    }

    if (size() > 0) {
      long last = getKey(0);
      for (int i = 1; i < size(); i++) {
        long tmp = getKey(i);
        if (tx.getTree().getComparator().compareLongKeys(tx, last, tmp) > 0) {
          ok = false;
          errors.add(new VerifyError(this, "Node key ordering error."));
        }
        last = tmp;
      }
    }

    if (!isLeaf()) {
      long last = tx.readNode(getPointer(0)).getKey(0);
      for (int i = 0; i < size(); i++) {
        long tmp = getKey(i);
        if (tx.getTree().getComparator().compareLongKeys(tx, last, tmp) > 0) {
          ok = false;
          errors.add(new VerifyError(this, "Node key child ordering error."));
        }
        last = tx.readNode(getPointer(i + 1)).getKey(0);
      }
    }
    return ok;
  }
}
