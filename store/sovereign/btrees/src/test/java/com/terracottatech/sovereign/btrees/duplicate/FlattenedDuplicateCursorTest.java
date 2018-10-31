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
import com.terracottatech.sovereign.btrees.bplustree.appendonly.ABPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.BPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.memory.PagedMemorySpace;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FlattenedDuplicateCursorTest {

  private DuplicateBPlusTree dupTree;

  private static PagedMemorySpace bufferStore() {
    return new PagedMemorySpace(PageSourceLocation.heap(), 4096, 256 * 1024, 1024 * 1024 * 1024);
  }

  @Before
  public void before() throws IOException {
    PagedMemorySpace store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    this.dupTree = new DuplicateBPlusTree(bplustree);
  }

  @After
  public void after() throws IOException {
    dupTree.close();
  }

  @Test
  public void testForwardTraversalWithDups() throws IOException {
    DuplicateWriteTx wtx = dupTree.writeTx();
    for (int i = 0; i < 100; i++) {
      wtx.insert(i, i);
      wtx.insert(i, i + 1);
      wtx.insert(i, i + 2);
    }
    wtx.commit(CommitType.DURABLE);
    wtx.close();
    DuplicateReadTx rtx = dupTree.readTx();
    FlattenedDuplicateCursor flat = rtx.cursor().flat();
    flat.first();
    int i = 0;
    while (flat.hasNext()) {
      if (flat.hasNext()) {
        BtreeEntry one = flat.next();
        assertThat(one.getKey(), is((long) i));
        assertThat(one.getValue(), is((long) i));
      }
      if (flat.hasNext()) {
        BtreeEntry one = flat.next();
        assertThat(one.getKey(), is((long) i));
        assertThat(one.getValue(), is((long) (i + 1)));
      }
      if (flat.hasNext()) {
        BtreeEntry one = flat.next();
        assertThat(one.getKey(), is((long) i));
        assertThat(one.getValue(), is((long) (i + 2)));
      }
      i++;
    }
    assertThat(i, is(100));
    rtx.close();
  }

  @Test
  public void testBackwardTraversalWithDups() throws IOException {
    DuplicateWriteTx wtx = dupTree.writeTx();
    for (int i = 0; i < 100; i++) {
      wtx.insert(i, i);
      wtx.insert(i, i + 1);
      wtx.insert(i, i + 2);
    }
    wtx.commit(CommitType.DURABLE);
    wtx.close();
    DuplicateReadTx rtx = dupTree.readTx();
    FlattenedDuplicateCursor flat = rtx.cursor().flat();
    flat.last();
    int i = 99;
    while (flat.hasPrevious()) {
      if (flat.hasPrevious()) {
        BtreeEntry one = flat.previous();
        assertThat(one.getKey(), is((long) i));
        assertThat(one.getValue(), is((long) (i + 2)));
      }
      if (flat.hasPrevious()) {
        BtreeEntry one = flat.previous();
        assertThat(one.getKey(), is((long) i));
        assertThat(one.getValue(), is((long) (i + 1)));
      }
      if (flat.hasPrevious()) {
        BtreeEntry one = flat.previous();
        assertThat(one.getKey(), is((long) i));
        assertThat(one.getValue(), is((long) i));
      }
      i--;
    }
    assertThat(i, is(-1));
    rtx.close();
  }

  @Test
  public void testTraversalScrubbing() throws IOException {
    DuplicateWriteTx wtx = dupTree.writeTx();
    // 0 0, 0 1, 1 1, 2 2, 2 3, 3 3, 4 4, 4 5, 5 5, 6 6, 6 7, 7 7, ...
    for (int i = 0; i < 20; i++) {
      wtx.insert(i, i);
      if ((i % 2) == 0) {
        wtx.insert(i, i + 1);
      }
    }
    wtx.commit(CommitType.DURABLE);
    wtx.close();
    DuplicateReadTx rtx = dupTree.readTx();
    FlattenedDuplicateCursor flat = rtx.cursor().flat();
    flat.first();
    System.out.println(flat.next());
    System.out.println(flat.next());
    System.out.println(flat.next());

    // should be at 2 2
    nextIs(flat, 2, 2);
    prevIs(flat, 2, 2);
    prevIs(flat, 1, 1);
    nextIs(flat, 1, 1);
    prevIs(flat, 0, 1);

    rtx.close();
  }

  private void prevIs(FlattenedDuplicateCursor flat, long k, long v) {
    BtreeEntry ent = flat.previous();
    assertThat(ent.getKey(), is(k));
    assertThat(ent.getValue(), is(v));
  }

  private void nextIs(FlattenedDuplicateCursor flat, long k, long v) {
    BtreeEntry ent = flat.next();
    assertThat(ent.getKey(), is(k));
    assertThat(ent.getValue(), is(v));
  }

  @Test
  public void testMoveToLastNoDups() throws IOException {
    DuplicateWriteTx wtx = dupTree.writeTx();
    for (int i = 0; i < 100; i++) {
      wtx.insert(i, i);
    }
    wtx.commit(CommitType.DURABLE);
    wtx.close();
    DuplicateReadTx rtx = dupTree.readTx();
    FlattenedDuplicateCursor flat = rtx.cursor().flat();
    flat.moveTo(99l);
    Assert.assertThat(flat.hasNext(), is(true));
    BtreeEntry foo = flat.next();
    rtx.close();
  }
}
