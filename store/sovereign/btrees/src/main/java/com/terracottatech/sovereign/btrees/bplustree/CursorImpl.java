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
import com.terracottatech.sovereign.btrees.bplustree.model.Cursor;
import com.terracottatech.sovereign.btrees.bplustree.model.Node;
import com.terracottatech.sovereign.btrees.bplustree.model.Tx;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;

/**
 * An implementation of the Cursor interface for Appending B+Tree's. Not thread safe.
 *
 * @author cschanck
 */
public final class CursorImpl implements Cursor {
  private static final Finger AFTER_END = new Finger();
  private static final Finger BEFORE_BEGIN = new Finger();
  private Tx<?> tx;
  private Finger nextFinger = null;
  private Finger previousFinger = null;
  private long workingRevision = -1L;
  private boolean lastMoveMatched = false;
  private boolean lastScanMatched = false;

  /**
   * Instantiates a new Cursor impl.
   *
   * @param tx the tx
   * @throws IOException the iO exception
   */
  public CursorImpl(Tx<?> tx) throws IOException {
    this.tx = tx;
    first();
  }

  /**
   * Instantiates a new Cursor impl.
   *
   * @param tx the tx
   * @param start the start
   * @throws IOException the iO exception
   */
  public CursorImpl(Tx<?> tx, long start) throws IOException {
    this.tx = tx;
    workingRevision = tx.getWorkingRevision();
    scanTo(start);
  }

  private void checkStable() {
    if (tx.getWorkingRevision() != workingRevision) {
      throw new ConcurrentModificationException("expected: " + workingRevision + " vs: " + tx.getWorkingRevision());
    }
  }

  private void ensureNextFinger() {
    if (nextFinger == null) {
      try {
        nextFinger = getNextFinger(previousFinger);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void ensurePreviousFinger() {
    if (previousFinger == null) {
      try {
        previousFinger = getPreviousFinger(nextFinger);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean moveTo(Object start) throws IOException {
    if (start != null) {
      nextFinger = tx.getTree().searchForNode((Tx) tx, start);
      lastMoveMatched = nextFinger.matched();
      previousFinger = null;
      workingRevision = tx.getWorkingRevision();
      if (!nextFinger.getLast().isIndexValid()) {
        nextFinger = ensureFinger(nextFinger);
      }
      return lastMoveMatched;
    } else {
      first();
      return true;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean scanTo(long start) throws IOException {
    nextFinger = tx.getTree().scanForNode((Tx) tx, start);
    previousFinger = null;
    lastScanMatched = nextFinger.matched();
    workingRevision = tx.getWorkingRevision();
    if (!nextFinger.getLast().isIndexValid()) {
      nextFinger = getNextFinger(nextFinger);
    }
    return lastScanMatched;
  }

  @Override
  public boolean wasLastMoveMatched() {
    return lastMoveMatched;
  }

  @Override
  public boolean wasLastScanMatched() {
    return lastScanMatched;
  }

  @Override
  public boolean resetInPlace(Tx<?> newTx) throws IOException {
    if (tx.getWorkingRevision() == workingRevision) {
      this.tx = newTx;
      return true;
    }
    first();
    return false;
  }

  @Override
  public Cursor first() throws IOException {
    previousFinger = BEFORE_BEGIN;
    nextFinger = null;
    workingRevision = tx.getWorkingRevision();
    return this;
  }

  private Finger getFirstFinger() throws IOException {
    Finger finger = new Finger();
    Node cn = tx.readNode(tx.getWorkingRootOffset());
    while (true) {
      if (cn.isLeaf()) {
        TreeLocation loc = new TreeLocation(cn, 0);
        finger.add(loc);
        break;
      } else {
        TreeLocation loc = new TreeLocation(cn, 0);
        finger.add(loc);
        cn = tx.readNode(cn.getPointer(0));
      }
    }
    return finger;
  }

  private Finger getLastFinger() throws IOException {
    Finger finger = new Finger();
    Node cn = tx.readNode(tx.getWorkingRootOffset());
    while (true) {
      if (cn.isLeaf()) {
        TreeLocation loc = new TreeLocation(cn, cn.getSize() - 1);
        finger.add(loc);
        break;
      } else {
        TreeLocation loc = new TreeLocation(cn, cn.getSize());
        finger.add(loc);
        cn = tx.readNode(cn.getPointer(cn.getSize()));
      }
    }
    return finger;
  }

  @Override
  public Cursor last() throws IOException {
    nextFinger = AFTER_END;
    previousFinger = null;
    workingRevision = tx.getWorkingRevision();
    return this;
  }

  @Override
  public boolean hasNext() {
    checkStable();
    ensureNextFinger();
    return nextFinger != AFTER_END && nextFinger.getLast().getLeafEntry() != null;
  }

  @Override
  public boolean hasPrevious() {
    checkStable();
    ensurePreviousFinger();
    return previousFinger != BEFORE_BEGIN && previousFinger.getLast().getLeafEntry() != null;
  }

  @Override
  public BtreeEntry previous() {
    BtreeEntry ret = peekPrevious();
    if (ret != null) {
      try {
        previousFinger = getPreviousFinger(previousFinger);
        nextFinger = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return ret;
    }
    throw new NoSuchElementException();
  }

  public BtreeEntry next() {
    BtreeEntry ret = peekNext();
    if (ret != null) {
      try {
        nextFinger = getNextFinger(nextFinger);
        previousFinger = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return ret;
    }
    throw new NoSuchElementException();
  }

  @Override
  public BtreeEntry peekNext() {
    checkStable();
    if (hasNext()) {
      return nextFinger.getLast().getLeafEntry();
    }
    return null;
  }

  @Override
  public BtreeEntry peekPrevious() {
    checkStable();
    if (hasPrevious()) {
      return previousFinger.getLast().getLeafEntry();
    }
    return null;
  }

  private Finger getPreviousFinger(Finger finger) throws IOException {
    if (finger == BEFORE_BEGIN) {
      throw new IllegalStateException();
    } else if (finger == AFTER_END) {
      return getLastFinger();
    } else {
      return retreatFinger(finger);
    }
  }

  private Finger getNextFinger(Finger finger) throws IOException {
    if (finger == BEFORE_BEGIN) {
      return getFirstFinger();
    } else if (finger == AFTER_END) {
      throw new IllegalStateException();
    } else {
      return advanceFinger(finger);
    }
  }

  private Finger ensureFinger(Finger sourceFinger) throws IOException {
    if (sourceFinger.isEmpty()) {
      return AFTER_END;
    } else {
      Finger finger = new Finger(sourceFinger);
      if (finger.getLast().getNode().isLeaf()) {

        int index = finger.getLast().getCurrentIndex();
        if (index < 0) {
          index = ~index;
        }
        if (index < finger.getLast().getNode().size()) {
          finger.getLast().setCurrentIndex(index);
          return finger;
        } else {
          if (finger.size() > 1) {
            finger.removeLast();
            return advanceFinger(finger);
          } else {
            return AFTER_END;
          }
        }
      } else {
        int index = finger.getLast().getCurrentIndex();
        if (index < 0) {
          index = ~index;
        }
        if (++index <= finger.getLast().getNode().size()) {

          finger.getLast().setCurrentIndex(index);
          long nextNodeOffset = finger.getLast().getNode().getPointer(index);
          while (nextNodeOffset >= 0) {
            Node nn = tx.readNode(nextNodeOffset);
            TreeLocation loc = new TreeLocation(nn, 0);
            finger.addLast(loc);
            if (nn.isLeaf()) {
              return finger;
            } else {
              nextNodeOffset = nn.getPointer(0);
            }
          }
          return AFTER_END;
        } else {
          finger.removeLast();
          return advanceFinger(finger);
        }
      }
    }
  }

  private Finger advanceFinger(Finger sourceFinger) throws IOException {
    if (sourceFinger.isEmpty()) {
      return AFTER_END;
    } else {
      Finger finger = new Finger(sourceFinger);
      if (finger.getLast().getNode().isLeaf()) {

        int index = finger.getLast().getCurrentIndex();
        if (index < 0) {
          index = ~index;
        }
        if (++index < finger.getLast().getNode().size()) {
          finger.getLast().setCurrentIndex(index);
          return finger;
        } else {
          if (finger.size() > 1) {
            finger.removeLast();
            return advanceFinger(finger);
          } else {
            return AFTER_END;
          }
        }
      } else {
        int index = finger.getLast().getCurrentIndex();
        if (index < 0) {
          index = ~index;
        }
        if (++index <= finger.getLast().getNode().size()) {

          finger.getLast().setCurrentIndex(index);
          long nextNodeOffset = finger.getLast().getNode().getPointer(index);
          while (nextNodeOffset >= 0) {
            Node nn = tx.readNode(nextNodeOffset);
            TreeLocation loc = new TreeLocation(nn, 0);
            finger.addLast(loc);
            if (nn.isLeaf()) {
              return finger;
            } else {
              nextNodeOffset = nn.getPointer(0);
            }
          }
          return AFTER_END;
        } else {
          finger.removeLast();
          return advanceFinger(finger);
        }
      }
    }
  }

  private Finger retreatFinger(Finger sourceFinger) throws IOException {
    if (sourceFinger.isEmpty()) {
      return BEFORE_BEGIN;
    } else {
      Finger finger = new Finger(sourceFinger);
      if (finger.getLast().getNode().isLeaf()) {

        int index = finger.getLast().getCurrentIndex();
        if (index < 0) {
          index = ~index;
        }
        if (index > 0) {
          finger.getLast().setCurrentIndex(--index);
          return finger;
        } else {
          if (finger.size() > 1) {
            finger.removeLast();
            return retreatFinger(finger);
          } else {
            return BEFORE_BEGIN;
          }
        }
      } else {
        int index = finger.getLast().getCurrentIndex();
        if (index < 0) {
          index = ~index;
        }
        if (index > 0) {

          finger.getLast().setCurrentIndex(--index);
          long nextNodeOffset = finger.getLast().getNode().getPointer(index);
          while (nextNodeOffset >= 0) {
            Node nn = tx.readNode(nextNodeOffset);
            if (nn.isLeaf()) {
              TreeLocation loc = new TreeLocation(nn, nn.getSize() - 1);
              finger.addLast(loc);
              return finger;
            } else {
              TreeLocation loc = new TreeLocation(nn, nn.getSize());
              finger.addLast(loc);
              nextNodeOffset = nn.getPointer(nn.getSize());
            }
          }
          return BEFORE_BEGIN;
        } else {
          finger.removeLast();
          return retreatFinger(finger);
        }
      }
    }
  }
}
