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

import com.terracottatech.sovereign.btrees.bplustree.model.BPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.IngestHandle;
import com.terracottatech.sovereign.btrees.bplustree.model.MutableNode;
import com.terracottatech.sovereign.btrees.bplustree.model.WriteTx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IngestHandleImpl implements IngestHandle {
  private final WriteTx<?> tx;
  private final BPlusTree<?> tree;
  private final int loadMax;
  private ArrayList<Long> bottom = new ArrayList<>();
  private MutableNode current = null;
  private MutableNode elder = null;
  private long size = 0;
  private int height = 0;
  private long lastK;

  public IngestHandleImpl(WriteTx<?> wtx) {
    this.tx = wtx;
    this.tree = wtx.getTree();
    this.loadMax = (3 * tx.getTree().getMaxKeysPerNode()) / 4;
    tx.begin();
    if (tx.size() != 0) {
      tx.close();
      throw new IllegalStateException("Tree must be empty");
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void ingest(long k, long v) throws IOException {
    ensureRoom();
    if (size > 0 && tree.getComparator().compareLongKeys((WriteTx) this.tx, k, lastK) <= 0) {
      throw new IllegalStateException();
    }
    lastK = k;
    current.insertKeyValueAt(current.size(), k, v);
    size++;
  }

  private void ensureRoom() throws IOException {
    if (current == null) {
      current = tx.createNewNode(true);
    } else if (current.size() > loadMax) {
      if(elder!=null) {
        long addr = elder.flush();
        bottom.add(addr);
      }
      elder=current;
      current = tx.createNewNode(true);
    }
  }

  @Override
  public WriteTx<?> endBatch() throws IOException {
    if (current != null && elder != null) {
      if (current.size() == 0) {
        flushElderAndCurrent();
      } else if (current.size() < current.minSize()) {
        if (current.size() + elder.size() < tx.getTree().getMaxKeysPerNode()) {
          while (current.size() > 0) {
//            System.out.println("1: " + elder.size() + " :: " + current.size());
            elder.insertKeyValueAt(elder.size(), current.getKey(0), current.getValue(0));
            current.removeKeyValueAt(0);
          } flushElderAndCurrent();
        } else {
          while (current.size() < current.minSize()) {
//            System.out.println("2: " + elder.size() + " :: " + current.size());
            current.insertKeyValueAt(0, elder.getKey(elder.size() - 1), elder.getValue(elder.size() - 1));
            elder.removeKeyValueAt(elder.size() - 1);
            if (elder.size() < elder.minSize()) {
              throw new IllegalStateException();
            }
          }
          flushElderAndCurrent();
        }
      } else {
        flushElderAndCurrent();
      }
    } else if (current != null) {
      if (current.size() > 0) {
        long addr = current.flush();
        bottom.add(addr);
        current = null;
      }
    } else {
      return tx;
    }

    if (bottom.isEmpty()) {
      return tx;
    }

    long rootAddress = buildUntilRoot();
    tx.recordSizeDelta(size);
    tx.recordHeightDelta(height);
    tx.setWorkingRootOffset(rootAddress);
    return tx;
  }

  private void flushElderAndCurrent() throws IOException {
    long addr = elder.flush();
    bottom.add(addr);
    if (current.size() > 0) {
      addr = current.flush();
      bottom.add(addr);
    }
    elder = null;
    current = null;
  }

  private long buildUntilRoot() throws IOException {
    ArrayList<Long> prior = bottom;
    height = 1;
    while (prior.size() > 1) {
      ArrayList<Long> newLevel = buildOneLevel(prior);
      height++;
      prior.clear();
      prior = newLevel;
    }

    return prior.get(0);
  }

  private ArrayList<Long> buildOneLevel(ArrayList<Long> prior) throws IOException {
    ArrayList<Long> newLevel = new ArrayList<>();

    int position = 0;
    int remaining = prior.size();

    while (remaining > 0) {
      int count = getPointerCountInThisBranch(remaining);
      createBranchNode(newLevel, prior, position, count);
      position += count;
      remaining -= count;
    }

    return newLevel;
  }

  private int getPointerCountInThisBranch(int distanceToEnd) {
    return getKeyCountInThisBranch(distanceToEnd - 1) + 1;
  }

  private int getKeyCountInThisBranch(int distanceToEnd) {
    if (distanceToEnd >= loadMax * 2) {
      return loadMax;
    }

    if (distanceToEnd >= tx.getTree().getMaxKeysPerNode()) {
      return distanceToEnd - (tx.getTree().getMaxKeysPerNode() / 2);
    }

    return distanceToEnd;
  }

  private void createBranchNode(List<Long> newLevel, List<Long> prior, int position, int pointerCount) throws IOException {
    if (pointerCount == 0) {
      throw new AssertionError("Attempt to make a branch with no children");
    }

    MutableNode branch = tx.createNewNode(false);

    for (int i = 0; i < pointerCount; i++) {
      MutableNode lowerNode = tx.readNode(prior.get(position + i));
      long offset = lowerNode.getOffset();

      if (i == 0) {
        branch.setPointer(0, offset);
      } else {
        branch.insertKeyPointerAt(i - 1, lowerNode.smallestKeyUnder(), offset);
      }
    }

    long addr = branch.flush();
    newLevel.add(addr);
  }
}