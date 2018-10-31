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

import com.terracottatech.sovereign.btrees.bplustree.CommitType;
import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;
import com.terracottatech.sovereign.btrees.bplustree.model.WriteTx;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author cschanck
 */
public class DuplicateWriteTx extends DuplicateReadTx {
  private final WriteTx<?> writeTx;

  public DuplicateWriteTx(DuplicateBPlusTree tree, WriteTx<?> writeTx) {
    super(tree, writeTx);
    this.writeTx = writeTx;
  }

  /**
   * Throw away the current write tx.
   *
   * @throws java.io.IOException
   */
  public void discard() throws IOException {
    writeTx.discard();
  }

  /**
   * Commit.
   *
   * @param commitType
   * @throws IOException
   */
  public void commit(CommitType commitType) throws IOException {
    writeTx.commit(commitType);
  }

  private void checkValue(long value) {
    if ((value & DuplicateBPlusTree.LIST_BIT) != 0L) {
      throw new IllegalArgumentException("Can use top bit for value in Duplicate Tree");
    }
  }

  /**
   * Insert, replacing as needed.
   *
   * @param key
   * @param value
   * @throws IOException
   */
  public void insert(long key, long value) throws IOException {
    checkValue(value);
    BtreeEntry ret = writeTx.insertIfAbsent(key, value);
    if (ret != null) {
      LongList ll;
      if (isList(ret.getValue())) {
        // already a list.
        ll = readList(ret.getValue() & DuplicateBPlusTree.VALID_MASK, false);
        ll.add(value);
        writeTx.queueFree(dupSpace, ret.getValue() & DuplicateBPlusTree.VALID_MASK, writeTx.getWorkingRevision());
      } else {
        // need a list now.
        ll = new LongList(dupSpace.getPageSize());
        ll.add(ret.getValue());
        ll.add(value);
      }
      long newAddr = dupSpace.append(ll.marshalForWrite());
      newAddr = newAddr | DuplicateBPlusTree.LIST_BIT;
      writeTx.replace(key, newAddr);
    }
  }

  /**
   * Insert only if key not present.
   *
   * @param key
   * @param value
   * @return
   * @throws IOException
   */
  public DuplicateEntry insertIfAbsent(long key, long value) throws IOException {
    checkValue(value);
    BtreeEntry ret = writeTx.insertIfAbsent(key, value);
    if (ret != null) {
      return renderValue(ret.getKey(), ret.getValue(), true);
    }
    return null;
  }

  /**
   * Delete an object (key) from the tree.
   *
   * @param keyObject
   * @return
   * @throws IOException
   */
  List<Long> deleteAll(long keyObject) throws IOException {
    BtreeEntry ret = writeTx.delete(keyObject);
    if (ret != null) {
      DuplicateEntry list = renderValue(ret.getKey(), ret.getValue(), true);
      if (isList(ret.getValue())) {
        writeTx.queueFree(dupSpace, ret.getValue() & DuplicateBPlusTree.VALID_MASK, writeTx.getWorkingRevision());
      }
      return list;
    }
    return Collections.emptyList();
  }

  /**
   * Delete an object (key) from the tree, only if the specified key exists mapped to the specified value.
   *
   * @param keyObject
   * @param valueObject
   * @return
   * @throws IOException
   */
  public BtreeEntry delete(long keyObject, long valueObject) throws IOException {
    checkValue(valueObject);
    BtreeEntry ret1 = writeTx.delete(keyObject, valueObject);
    if (ret1 != null) {
      return ret1;
    }
    Long ret = writeTx.find(keyObject);
    if (ret != null) {
      if (isList(ret)) {
        // a list.
        LongList ll = readList(ret & DuplicateBPlusTree.VALID_MASK, false);
        if (ll.remove(valueObject)) {
          long newAddr = dupSpace.append(ll.marshalForWrite());
          newAddr = newAddr | DuplicateBPlusTree.LIST_BIT;
          writeTx.replace(keyObject, newAddr);
          writeTx.queueFree(dupSpace, ret & DuplicateBPlusTree.VALID_MASK, writeTx.getWorkingRevision());
          return new BtreeEntry(keyObject, valueObject);
        }
      } else {
        return new BtreeEntry(keyObject, ret);
      }
    }
    return null;
  }
}
