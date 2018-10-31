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

import com.terracottatech.sovereign.btrees.bplustree.appendonly.ABPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.BPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.btrees.stores.SimpleStore;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.memory.PagedMemorySpace;
import org.hamcrest.core.IsNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;

public class DuplicateBPlusTreeTest {

  private static PagedMemorySpace bufferStore() {
    return new PagedMemorySpace(PageSourceLocation.heap(), 4096, 256 * 1024, 1024 * 1024);
  }

  @Test
  public void testCreation() throws IOException {
    PagedMemorySpace store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    DuplicateBPlusTree dupTree = new DuplicateBPlusTree(bplustree);
    dupTree.close();
  }

  @Test
  public void testCRUDNoDups() throws IOException {
    PagedMemorySpace store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    DuplicateBPlusTree dupTree = new DuplicateBPlusTree(bplustree);
    DuplicateWriteTx wtx = dupTree.writeTx();
    for (int i = 0; i < 100; i++) {
      wtx.insert(i, i);
    }
    wtx.commit();
    DuplicateReadTx rtx = dupTree.readTx();
    for (int i = 0; i < 100; i++) {
      DuplicateEntry got = rtx.find((long) i);
      Assert.assertThat(got.size(), is(1));
      Assert.assertThat(got.get(0).longValue(), is((long) i));
    }
    wtx.delete(5, 5);
    wtx.commit();
    rtx.commit();
    for (int i = 0; i < 100; i++) {
      DuplicateEntry got = rtx.find((long) i);
      if (i == 5) {
        Assert.assertThat(got, nullValue());
      } else {
        Assert.assertThat(got.size(), is(1));
        Assert.assertThat(got.get(0).longValue(), is((long) i));
      }
    }
    dupTree.close();
  }

  @Test
  public void testCRUDSingleDup() throws IOException {
    PagedMemorySpace store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    DuplicateBPlusTree dupTree = new DuplicateBPlusTree(bplustree);
    DuplicateWriteTx wtx = dupTree.writeTx();
    // insert 1
    wtx.insert(1, 1);
    wtx.commit();
    Assert.assertThat(wtx.find((long) 1).size(), is(1));
    wtx.insert(1, 2);
    wtx.commit();
    Assert.assertThat(wtx.find((long) 1).size(), is(2));
    BtreeEntry ret = wtx.delete(1, 1);
    Assert.assertThat(ret, notNullValue());
    wtx.commit();
    Assert.assertThat(wtx.find((long) 1).size(), is(1));
    ret = wtx.delete(1, 3);
    Assert.assertThat(ret, nullValue());
    wtx.commit();
    Assert.assertThat(wtx.find((long) 1).size(), is(1));

    dupTree.close();
  }

  @Test
  public void testCRUDManyDups() throws IOException {
    PagedMemorySpace store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    DuplicateBPlusTree dupTree = new DuplicateBPlusTree(bplustree);
    DuplicateWriteTx wtx = dupTree.writeTx();
    for (int i = 0; i < 100; i++) {
      wtx.insert(i, i * 100 + 1);
      wtx.insert(i, i * 100 + 2);
      wtx.insert(i, i * 100 + 3);
    }
    wtx.commit();
    for (int i = 0; i < 100; i++) {
      Assert.assertThat(wtx.find((long) i).size(), is(3));
    }
    wtx.commit();
    for (int i = 0; i < 100; i++) {
      wtx.delete(i, i * 100 + 2);
    }
    wtx.commit();
    for (int i = 0; i < 100; i++) {
      List<Long> ret = wtx.find((long) i);
      Assert.assertThat(ret.size(), is(2));
      Assert.assertThat(ret.contains((long) i * 100 + 1), is(true));
      Assert.assertThat(ret.contains((long) i * 100 + 3), is(true));
    }

    dupTree.close();
  }

  @Test
  public void testCRUDPutIfAbsent() throws IOException {
    PagedMemorySpace store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    DuplicateBPlusTree dupTree = new DuplicateBPlusTree(bplustree);
    DuplicateWriteTx wtx = dupTree.writeTx();
    // insert 1
    for (int i = 0; i < 100; i++) {
      wtx.insert(i, i * 100 + 1);
      wtx.insert(i, i * 100 + 2);
      wtx.insert(i, i * 100 + 3);
    }
    wtx.commit();

    DuplicateEntry got = wtx.insertIfAbsent(10, 10);
    Assert.assertThat(got.get(0), is(1001l));
    Assert.assertThat(got.get(1), is(1002l));
    Assert.assertThat(got.get(2), is(1003l));

    got = wtx.insertIfAbsent(211, 10);
    Assert.assertThat(got, nullValue());

    dupTree.close();
  }

  @Test
  public void testDupSpacePageOverflowOps() throws IOException {
    PagedMemorySpace store = bufferStore();
    PagedMemorySpace dupSpace = new PagedMemorySpace(PageSourceLocation.heap(), LongListProxy.byteSizeForListCount(5),
      256 * 1024, 1024 * 1024);
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, new SimpleStore[]{dupSpace}, null, 10, BPlusTree.DEFAULT_LONG_HANDLER,
      0);
    DuplicateBPlusTree dupTree = new DuplicateBPlusTree(bplustree, dupSpace);
    DuplicateWriteTx wtx = dupTree.writeTx();
    // insert 1
    wtx.insert(1, 100);
    wtx.insert(1, 101);
    wtx.insert(1, 102);
    wtx.insert(1, 103);
    wtx.insert(1, 104);
    wtx.commit();
    Assert.assertThat(dupSpace.getStats().getUserBytes(), not(0l));
    DuplicateEntry got = wtx.find(1l);
    Assert.assertThat(got.size(), is(5));
    Assert.assertThat(got.get(0), is(100l));
    Assert.assertThat(got.get(1), is(101l));
    Assert.assertThat(got.get(2), is(102l));
    Assert.assertThat(got.get(3), is(103l));
    Assert.assertThat(got.get(4), is(104l));
    wtx.commit();
    wtx.insert(1, 500);
    wtx.insert(1, 501);
    wtx.commit();
    got = wtx.find(1l);
    Assert.assertThat(got.size(), is(7));
    Assert.assertThat(got.get(0), is(100l));
    Assert.assertThat(got.get(1), is(101l));
    Assert.assertThat(got.get(2), is(102l));
    Assert.assertThat(got.get(3), is(103l));
    Assert.assertThat(got.get(4), is(104l));
    Assert.assertThat(got.get(5), is(500l));
    Assert.assertThat(got.get(6), is(501l));

    BtreeEntry delret = wtx.delete(1l, 103l);
    Assert.assertThat(delret.getKey(), is(1l));
    Assert.assertThat(delret.getValue(), is(103l));
    wtx.commit();
    got = wtx.find(1l);
    Assert.assertThat(got.size(), is(6));
    Assert.assertThat(got.get(0), is(100l));
    Assert.assertThat(got.get(1), is(101l));
    Assert.assertThat(got.get(2), is(102l));
    Assert.assertThat(got.get(3), is(104l));
    Assert.assertThat(got.get(4), is(500l));
    Assert.assertThat(got.get(5), is(501l));
    wtx.commit();

    wtx.deleteAll(1l);
    wtx.commit();
    Assert.assertThat(wtx.find(1l), IsNull.nullValue());
    Assert.assertThat(dupSpace.getStats().getUserBytes(), is(0l));
    wtx.close();
    dupTree.close();
  }
}
