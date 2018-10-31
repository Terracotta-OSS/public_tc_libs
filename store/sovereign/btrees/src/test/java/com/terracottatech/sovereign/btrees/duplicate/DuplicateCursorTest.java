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
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.memory.PagedMemorySpace;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class DuplicateCursorTest {

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
  public void testMoveToLast() throws IOException {
    DuplicateWriteTx wtx = dupTree.writeTx();
    for (int i = 0; i < 100; i++) {
      wtx.insert(2 * i, i + 100);
      wtx.insert(2 * i, i + 200);
    }
    wtx.commit();
    DuplicateReadTx rtx = dupTree.readTx();
    DuplicateCursor curs = rtx.cursor();
    curs.moveTo(198l);
    Assert.assertThat(curs.hasNext(),is(true));
  }

  @Test
  public void testCreationEmptyTree() throws IOException {
    DuplicateWriteTx rtx = dupTree.writeTx();
    DuplicateCursor curs = rtx.cursor();
    Assert.assertThat(curs, notNullValue());
    rtx.close();
  }

  @Test
  public void testIterateForward() throws IOException {
      DuplicateWriteTx wtx = dupTree.writeTx();
      for (int i = 0; i < 100; i++) {
        wtx.insert(2 * i, i + 100);
        wtx.insert(2 * i, i + 200);
      }
      wtx.commit();
      DuplicateReadTx rtx = dupTree.readTx();
      DuplicateCursor curs = rtx.cursor();
      curs.first();
    int i = 0;
    while (curs.hasNext()) {
      DuplicateEntry got = curs.next();
      Assert.assertThat(got.getKey(), is((long) (2 * i)));
      Assert.assertThat(got.size(), is(2));
      Assert.assertThat(got.get(0), is((long) (i + 100)));
      Assert.assertThat(got.get(1), is((long) (i + 200)));
      i++;
    }
    Assert.assertThat(i, is(100));
    rtx.close();
    wtx.close();
  }

  @Test
  public void testIterateScrub() throws IOException {
    DuplicateWriteTx wtx = dupTree.writeTx();
    for (int i = 0; i < 100; i++) {
      wtx.insert(i, i);
      wtx.insert(i, i + 1);
    }
    wtx.commit();
    DuplicateReadTx rtx = dupTree.readTx();
    for (int i = 0; i < 20; i++) {
      DuplicateCursor curs = rtx.cursor();
      scrubTest(curs, i);
    }

    rtx.close();
    wtx.close();
  }

  private void scrubTest(DuplicateCursor curs, int skip) throws IOException {
    curs.first();
    for (int i = 0; i < skip; i++) {
      curs.next();
    }

    long pos = skip;

    DuplicateEntry got = curs.next();
    Assert.assertThat(got.getKey(), is(pos));
    Assert.assertThat(got.size(), is(2));
    Assert.assertThat(got.get(0), is(pos));
    Assert.assertThat(got.get(1), is(pos+1));
    pos++;

    got = curs.next();
    Assert.assertThat(got.getKey(), is(pos));
    Assert.assertThat(got.size(), is(2));
    Assert.assertThat(got.get(0), is(pos));
    Assert.assertThat(got.get(1), is(pos+1));
    pos++;

    got = curs.next();
    Assert.assertThat(got.getKey(), is(pos));
    Assert.assertThat(got.size(), is(2));
    Assert.assertThat(got.get(0), is(pos));
    Assert.assertThat(got.get(1), is(pos+1));

    got = curs.previous();
    Assert.assertThat(got.getKey(), is(pos));
    Assert.assertThat(got.size(), is(2));
    Assert.assertThat(got.get(0), is(pos));
    Assert.assertThat(got.get(1), is(pos+1));
    pos--;

    got = curs.previous();
    Assert.assertThat(got.getKey(), is(pos));
    Assert.assertThat(got.size(), is(2));
    Assert.assertThat(got.get(0), is(pos));
    Assert.assertThat(got.get(1), is(pos+1));
    pos--;

    got = curs.previous();
    Assert.assertThat(got.getKey(), is(pos));
    Assert.assertThat(got.size(), is(2));
    Assert.assertThat(got.get(0), is(pos));
    Assert.assertThat(got.get(1), is(pos+1));

  }

  @Test
  public void testIterateBackward() throws IOException {
    DuplicateWriteTx wtx = dupTree.writeTx();
    for (int i = 0; i < 100; i++) {
      wtx.insert(2 * i, i + 100);
      wtx.insert(2 * i, i + 200);
    }
    wtx.commit();
    DuplicateReadTx rtx = dupTree.readTx();
    DuplicateCursor curs = rtx.cursor();
    curs.last();
    int i = 99;
    while (curs.hasPrevious()) {
      DuplicateEntry got = curs.previous();
      Assert.assertThat(got.getKey(), is((long) (2 * i)));
      Assert.assertThat(got.size(), is(2));
      Assert.assertThat(got.get(0), is((long) (i + 100)));
      Assert.assertThat(got.get(1), is((long) (i + 200)));
      i--;
    }
    Assert.assertThat(i, is(-1));
    rtx.close();
    wtx.close();
  }

  @Test
  public void testFindMisses() throws IOException {
    DuplicateWriteTx wtx = dupTree.writeTx();
    for (int i = 0; i < 100; i++) {
      wtx.insert(2 * i, i + 100);
      wtx.insert(2 * i, i + 200);
    }
    wtx.commit();
    DuplicateReadTx rtx = dupTree.readTx();
    DuplicateCursor curs = rtx.cursor();
    curs.moveTo(11l);
    Assert.assertThat(curs.wasLastMoveMatched(), is(false));
    Assert.assertThat(curs.next().getKey(), is(12l));
    wtx.close();
  }

  @Test
  public void testFindSucceeds() throws IOException {
    DuplicateWriteTx wtx = dupTree.writeTx();
    for (int i = 0; i < 100; i++) {
      wtx.insert(2 * i, i + 100);
      wtx.insert(2 * i, i + 200);
    }
    wtx.commit();
    DuplicateReadTx rtx = dupTree.readTx();
    DuplicateCursor curs = rtx.cursor();
    curs.moveTo(12l);
    Assert.assertThat(curs.wasLastMoveMatched(), is(true));
    Assert.assertThat(curs.next().getKey(), is(12l));
    wtx.close();
  }

}
