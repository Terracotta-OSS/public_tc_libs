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

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * @author cschanck
 */
public class FlattenedDuplicateCursor {

  private final DuplicateCursor base;
  private DuplicateEntry nextEntry = null;
  private int valueIndex = -1;
  private boolean lastOpWasForward = false;

  FlattenedDuplicateCursor(DuplicateCursor base) throws IOException {
    this.base = base;
  }

  private void loadNext(DuplicateEntry ent) {
    nextEntry = ent;
    valueIndex = 0;
  }

  private void clearState() {
    nextEntry = null;
    valueIndex = -1;
    lastOpWasForward = false;
  }

  private boolean canAdvance() {
    return (nextEntry != null && valueIndex < nextEntry.size() - 1) || base.hasNext();
  }

  private boolean canRetreat() {
    return (nextEntry != null && valueIndex > 0) || lastOpWasForward || base.hasPrevious();
  }

  private void advance() {
    if (nextEntry == null || valueIndex >= nextEntry.size() - 1) {
      if (base.hasNext()) {
        loadNext(base.next());
        lastOpWasForward = true;
      } else {
        clearState();
      }
    } else {
      valueIndex++;
    }
  }

  private void retreat() {
    if (nextEntry == null || valueIndex <= 0) {
      if (lastOpWasForward) {
        base.previous();
      }
      if (base.hasPrevious()) {
        loadNext(base.previous());
        valueIndex = nextEntry.size() - 1;
      } else {
        clearState();
      }
      lastOpWasForward = false;
    } else {
      valueIndex--;
    }
  }

  public FlattenedDuplicateCursor first() throws IOException {
    base.first();
    if (canAdvance()) {
      lastOpWasForward = true;
      loadNext(base.next());
    }
    return this;
  }

  public FlattenedDuplicateCursor last() throws IOException {
    base.last();
    clearState();
    lastOpWasForward = false;
    return this;
  }

  public boolean moveTo(Object key) throws IOException {
    boolean ret = base.moveTo(key);
    if (canAdvance()) {
      lastOpWasForward = true;
      loadNext(base.next());
    }
    return ret;
  }

  public boolean scanTo(long start) throws IOException {
    boolean ret = base.scanTo(start);
    if (canAdvance()) {
      lastOpWasForward = true;
      loadNext(base.next());
    }
    return ret;
  }

  public boolean wasLastMoveMatched() {
    return base.wasLastMoveMatched();
  }

  public boolean wasLastScanMatched() {
    return base.wasLastScanMatched();
  }

  public boolean hasNext() {
    return (nextEntry != null && valueIndex >= 0 && valueIndex < nextEntry.size()) || canAdvance();
  }

  public boolean hasPrevious() {
    return canRetreat();
  }

  public BtreeEntry next() {
    if (nextEntry != null && valueIndex < nextEntry.size()) {
      // cool
      BtreeEntry ent = new BtreeEntry(nextEntry.getKey(), nextEntry.get(valueIndex));
      advance();
      return ent;
    }
    throw new NoSuchElementException();
  }

  public BtreeEntry previous() {
    if (canRetreat()) {
      retreat();
      BtreeEntry ent = new BtreeEntry(nextEntry.getKey(), nextEntry.get(valueIndex));
      return ent;
    }
    throw new NoSuchElementException();
  }

}
