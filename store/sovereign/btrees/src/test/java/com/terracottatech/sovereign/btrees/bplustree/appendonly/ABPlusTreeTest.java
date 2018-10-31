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

import com.terracottatech.sovereign.btrees.bplustree.CommitType;
import com.terracottatech.sovereign.btrees.bplustree.CursorImpl;
import com.terracottatech.sovereign.btrees.bplustree.model.BPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;
import com.terracottatech.sovereign.btrees.bplustree.model.Cursor;
import com.terracottatech.sovereign.btrees.bplustree.model.Node;
import com.terracottatech.sovereign.btrees.bplustree.model.ReadTx;
import com.terracottatech.sovereign.btrees.bplustree.model.Tx;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.btrees.bplustree.model.WriteTx;
import com.terracottatech.sovereign.btrees.stores.SimpleStore;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.memory.PagedMemorySpace;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author cschanck
 */
public class ABPlusTreeTest {
  private File file;

  private static PagedMemorySpace bufferStore() {
    return new PagedMemorySpace(PageSourceLocation.heap(), 4096, 256 * 1024, 1024 * 1024 * 1024);
  }

  @Before
  public void before() throws IOException {
    //file = File.createTempFile("lob", "lob");
    file = new File("e:\\lobtest");
    file.delete();
  }

  @After
  public void after() {
    if (file.exists()) {
      // file.delete();
    }
  }

  @Test
  public void testCreateEmpty() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    Tx<TxDecorator> tx = bplustree.readTx();
    assertThat(tx.size(), is(0l));
    tx.close();
    bplustree.close();
  }

  @Test
  public void testAddToSingleNodeNoDupsAllowedCommitted() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    WriteTx<TxDecorator> tx = bplustree.writeTx();
    tx.insert(0, 10);
    tx.insert(4, 20);
    tx.insert(1, 30);
    tx.insert(3, 40);
    tx.insert(2, 50);
    tx.insert(4, 400);
    tx.commit(CommitType.DURABLE);
    assertThat(tx.find(0L), is(10l));
    assertThat(tx.find(1L), is(30l));
    assertThat(tx.find(2l), is(50l));
    assertThat(tx.find(3l), is(40l));
    assertThat(tx.find(4l), is(400l));
    Assert.assertNull(tx.find(5l));
    Assert.assertNull(tx.find(100l));
    assertThat(tx.size(), is(5l));
    bplustree.close();
  }

  @Test
  public void testAddToMultiNode() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    WriteTx<TxDecorator> tx = bplustree.writeTx();
    for (int i = 0; i < 100; i++) {
      tx.insert(i, i);
    }
    tx.commit(CommitType.DURABLE);
    for (int i = 0; i < 100; i++) {
      assertThat(tx.find((long) i), is((long) i));
    }
    assertThat(tx.size(), is(100L));

    tx.close();

  }

  @Test
  public void testCursorEmpty() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    Tx<TxDecorator> tx = bplustree.readTx();
    tx.commit();
    Cursor it = tx.cursor();
    assertThat(it.hasNext(), is(false));
    assertThat(it.hasPrevious(), is(false));
  }

  @Test
  public void testCursorForwardSingleNode() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 6; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    Cursor it = tx.cursor();
    int cnt = 0;
    Long last = null;
    for (; it.hasNext(); ) {
      cnt++;
      Long l = it.next().getKey();
      if (last != null) {
        assertThat(l, greaterThan(last));
      }
      last = l;
    }
    tx.close();
    assertThat(cnt, is(6));
    bplustree.close();
  }

  @Test
  public void testCursorBackwardSingleNode() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 6; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    Cursor it = tx.cursor();
    it.last();
    int cnt = 0;
    Long last = null;
    for (; it.hasPrevious(); ) {
      cnt++;
      Long l = it.previous().getKey();
      System.out.println(l);
      if (last != null) {
        assertThat(l, lessThan(last));
      }
      last = l;
    }
    tx.close();
    assertThat(cnt, is(6));
    bplustree.close();
  }

  @Test
  public void testCursorForwardMultiNode() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 200; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    Cursor it = tx.cursor();
    int cnt = 0;
    Long last = null;
    for (; it.hasNext(); ) {
      cnt++;
      Long l = it.next().getKey();
      if (last != null) {
        assertThat(l, greaterThan(last));
      }
      last = l;
    }
    assertThat(cnt, is(200));
    tx.close();
    bplustree.close();
  }

  @Test
  public void testCursorBackwardMultiNode() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 200; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    Cursor it = tx.cursor();
    it.last();
    int cnt = 0;
    Long last = null;
    for (; it.hasPrevious(); ) {
      cnt++;
      Long l = it.previous().getKey();
      if (last != null) {
        assertThat(l, lessThan(last));
      }
      last = l;
    }
    assertThat(cnt, is(200));
    tx.close();
    bplustree.close();
  }

  @Test
  public void testCursorSingleNodeForwardFloorKeyed() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 6; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    Cursor it = tx.cursor(3l);
    int cnt = 0;
    Long last = null;
    for (; it.hasNext(); ) {
      cnt++;
      Long l = it.next().getKey();
      if (last != null) {
        assertThat(l, greaterThan(last));
      }
      last = l;
    }
    tx.close();
    assertThat(cnt, is(3));
    bplustree.close();
  }

  @Test
  public void testCursorSingleNodeBackwardFloorKeyed() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 6; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    Cursor it = tx.cursor(3l);
    int cnt = 0;
    Long last = null;
    for (; it.hasPrevious(); ) {
      cnt++;
      Long l = it.previous().getKey();
      if (last != null) {
        assertThat(l, lessThan(last));
      }
      last = l;
    }
    tx.close();
    assertThat(cnt, is(3));
    bplustree.close();
  }

  @Test
  public void testCursorForwardMultiNodeFloorKeyed() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 20; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    Cursor it = tx.cursor(4l);
    int cnt = 0;
    Long last = null;
    for (; it.hasNext(); ) {
      cnt++;
      Long l = it.next().getKey();
      if (last != null) {
        assertThat(l, greaterThan(last));
      }
      last = l;
    }
    tx.close();
    assertThat(cnt, is(16));
    bplustree.close();
  }

  @Test
  public void testCursorBackwardMultiNodeFloorKeyed() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 20; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    Cursor it = tx.cursor(4l);
    int cnt = 0;
    Long last = null;
    for (; it.hasPrevious(); ) {
      cnt++;
      Long l = it.previous().getKey();
      if (last != null) {
        assertThat(l, lessThan(last));
      }
      last = l;
    }
    tx.close();
    assertThat(cnt, is(4));
    bplustree.close();
  }

  @Test
  public void testCursorScrubbingSingleNode() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 6; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    Cursor it = tx.cursor();
    BtreeEntry got = null;

    if (it.hasNext()) {
      got = it.next();
      assertThat(got.getKey(), is(0l));
      assertThat(got.getValue(), is(0l));
    }
    if (it.hasNext()) {
      got = it.next();
      assertThat(got.getKey(), is(1l));
      assertThat(got.getValue(), is(1l));
    }
    if (it.hasNext()) {
      got = it.next();
      assertThat(got.getKey(), is(2l));
      assertThat(got.getValue(), is(2l));
    }
    if (it.hasPrevious()) {
      got = it.previous();
      assertThat(got.getKey(), is(2l));
      assertThat(got.getValue(), is(2l));
    }
    if (it.hasPrevious()) {
      got = it.previous();
      assertThat(got.getKey(), is(1l));
      assertThat(got.getValue(), is(1l));
    }
    if (it.hasPrevious()) {
      got = it.previous();
      assertThat(got.getKey(), is(0l));
      assertThat(got.getValue(), is(0l));
    }
    tx.close();
    bplustree.close();
  }

  @Test
  public void testCursorScrubbingMultiNode() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 9; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    BtreeEntry got = null;

    for (int i = 0; i < 7; i++) {
      Cursor it = tx.cursor();
      scrubTest(it, i);
    }
    tx.close();
    bplustree.close();
  }

  @Test
  public void testCursorEndOfLeafMissTDB1372() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 20; i = i + 2) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    // bplustree.dump(System.out);
    ReadTx<TxDecorator> tx = bplustree.readTx();
    tx.begin();
    // find leftmost leaf node.
    Node n = tx.readNode(tx.getWorkingRootOffset());
    assertThat(n.isLeaf(), is(false));
    for (; ; ) {
      n = tx.readNode(n.getPointer(0));
      if (n.isLeaf()) {
        break;
      }
    }
    // find the last one
    long val = n.getValue(n.size() - 1) + 1;
    System.out.println("Last value in left leaf is: " + val);
    CursorImpl curs = new CursorImpl(tx);
    curs.moveTo(val);
    int cnt = 0;
    assertThat(curs.hasNext(), is(true));
    assertThat(curs.next().getKey(), is(val+1));
    tx.close();
    bplustree.close();
  }

  private void scrubTest(Cursor it, int skip) {
    BtreeEntry got;
    for (int i = 0; i < skip; i++) {
      it.next();
    }
    long pos = skip;

    if (it.hasNext()) {
      got = it.next();
      assertThat(got.getKey(), is(pos));
      assertThat(got.getValue(), is(pos++));
    }
    if (it.hasNext()) {
      got = it.next();
      assertThat(got.getKey(), is(pos));
      assertThat(got.getValue(), is(pos++));
    }
    if (it.hasNext()) {
      got = it.next();
      assertThat(got.getKey(), is(pos));
      assertThat(got.getValue(), is(pos));
    }
    if (it.hasPrevious()) {
      got = it.previous();
      assertThat(got.getKey(), is(pos));
      assertThat(got.getValue(), is(pos--));
    }
    if (it.hasPrevious()) {
      got = it.previous();
      assertThat(got.getKey(), is(pos));
      assertThat(got.getValue(), is(pos--));
    }
    if (it.hasPrevious()) {
      got = it.previous();
      assertThat(got.getKey(), is(pos));
      assertThat(got.getValue(), is(pos));
    }
  }

  @Test
  public void testCursorLastSingleNode() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 6; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    Cursor it = tx.cursor();
    it.moveTo(5l);
    Assert.assertThat(it.hasNext(), is(true));
    Assert.assertThat(it.next().getValue(), is(5l));
    Assert.assertThat(it.hasNext(), is(false));
  }

  @Test
  public void testCursorLastMultiNode() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 9; i++) {
        tx.insert(i, i);
      }
      tx.commit();
      tx.close();
    }
    Tx<TxDecorator> tx = bplustree.readTx();
    Cursor it = tx.cursor();
    it.moveTo(8l);
    Assert.assertThat(it.hasNext(), is(true));
    Assert.assertThat(it.next().getValue(), is(8l));
    Assert.assertThat(it.hasNext(), is(false));
  }

  @Test
  public void testDeleteFromSingleRoomyLeafNode() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 9; i++) {
        tx.insert(i, 10 + i);
      }
      tx.commit();
      tx.close();
    }

    WriteTx<TxDecorator> tx = bplustree.writeTx();
    BtreeEntry old = tx.delete(5L);
    assertThat(old.getValue(), is(15l));
    assertThat(tx.size(), is(8l));
    Assert.assertNull(tx.find(5l));
    store.close();
  }

  @Test
  public void testDeleteFromRoomyLeafNodeWithSibling() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 11; i++) {
        tx.insert(i, 10 + i);
      }
      tx.commit();
      tx.close();
    }

    WriteTx<TxDecorator> tx = bplustree.writeTx();
    BtreeEntry old = tx.delete(5L);
    assertThat(old.getValue(), is(15l));
    assertThat(tx.size(), is(10l));
    Assert.assertNull(tx.find(5l));
    Cursor c = tx.cursor();
    while (c.hasNext()) {
      assertThat(c.next().getKey(), not(is(5l)));
    }
  }

  @Test
  public void testDeleteFromLeafNodeStealFromLeftSibling() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 10; i++) {
        tx.insert(i, 10 + i);
      }
      for (int i = 1; i < 5; i++) {
        tx.insert(0 - i, 0 - (10 + i));
      }
      tx.commit();
      tx.close();
    }
    bplustree.dump(System.out);
    WriteTx<TxDecorator> tx = bplustree.writeTx();
    tx.delete(5L);
    BtreeEntry old = tx.delete(6L);
    assertThat(old.getValue(), is(16l));
    assertThat(tx.size(), is(12l));
    Assert.assertNull(tx.find(5l));
    Assert.assertNull(tx.find(6l));
    bplustree.dump(tx, System.out);
    Cursor c = tx.cursor();
    while (c.hasNext()) {
      assertThat(c.next().getKey(), not(is(6l)));
      assertThat(c.next().getKey(), not(is(5l)));
    }
    tx.commit(CommitType.DURABLE);
    tx.close();
    store.close();
  }

  @Test
  public void testDeleteFromLeafNodeStealFromRightSibling() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 10; i++) {
        tx.insert(i, 10 + i);
      }
      for (int i = 1; i < 5; i++) {
        tx.insert(0 - i, 0 - (10 + i));
      }
      tx.commit();
      tx.close();
    }
    bplustree.dump(System.out);
    WriteTx<TxDecorator> tx = bplustree.writeTx();
    tx.delete(-4L);

    tx.delete(3L);
    Assert.assertNull(tx.find(3l));

    tx.delete(-2L);
    Assert.assertNull(tx.find(-2l));

    tx.delete(0L);
    Assert.assertNull(tx.find(0l));

    tx.delete(1L);
    Assert.assertNull(tx.find(1l));

    tx.delete(2L);

    tx.commit(CommitType.DURABLE);
    tx.close();
    store.close();

  }

  @Test
  public void testDeleteFromLeafNodeMergeFromRightSibling() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 11; i++) {
        tx.insert(i, 10 + i);
      }
      tx.commit();
      tx.close();
    }
    bplustree.dump(System.out);

    WriteTx<TxDecorator> tx = bplustree.writeTx();
    tx.delete(0l);
    tx.delete(1l);
    tx.delete(2l);
    tx.delete(3l);
    tx.commit(CommitType.DURABLE);
    Assert.assertNull(tx.find(0l));
    Assert.assertNull(tx.find(1l));
    Assert.assertNull(tx.find(2l));
    Assert.assertNull(tx.find(3l));
    assertThat(tx.size(), is(7l));
    tx.close();
    store.close();
  }

  @Test
  public void testDeleteFromLeafNodeMergeIntoLeftSibling() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 12; i++) {
        tx.insert(i, 10 + i);
      }
      tx.commit();
      tx.close();
    }

    bplustree.dump(System.out);
    WriteTx<TxDecorator> tx = bplustree.writeTx();
    tx.delete(11l);
    tx.delete(10l);
    tx.delete(9l);
    tx.delete(8l);
    tx.delete(7l);
    tx.commit();
    tx.close();
    Tx<TxDecorator> rtx = bplustree.readTx();
    Assert.assertNull(rtx.find(11l));
    Assert.assertNull(rtx.find(9l));
    Assert.assertNull(rtx.find(8l));
    Assert.assertNull(rtx.find(7l));

    bplustree.dump(System.out);

  }

  @Test
  public void testMassDeletesFromFront() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    Long[] vals = new Long[100];
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 100; i++) {
        vals[i] = (long)i;
        tx.insert(i, 10 + i);
      }
      tx.commit();
      tx.close();
    }

    // delete from the front.
    for (int i = 0; i < 100; i++) {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      tx.delete((long) i);
      vals[i] = null;
      tx.commit();
      tx.close();
      Tx<TxDecorator> rtx = bplustree.readTx();
      treeVerify(rtx, vals);
      rtx.close();
    }
    assertThat(bplustree.getStats().branchLeftToRight(), is(0l));
    assertThat(bplustree.getStats().branchRightToLeft(), is(0l));
    assertThat(bplustree.getStats().branchNodeMerges(), greaterThan(0l));
    store.close();
  }

  private void treeVerify(Tx<TxDecorator> rtx, Long[] vals) throws IOException {
    // verify finds
    int targetSize = 0;
    for (int i = 0; i < vals.length; i++) {
      if (vals[i] != null) {
        Assert.assertNotNull(rtx.find((long) i));
        targetSize++;
      } else {
        Assert.assertNull(rtx.find((long) i));
      }
    }
    assertThat(rtx.size(), is((long) targetSize));
    // now, verify the cursor
    Cursor c = rtx.cursor();
    while (c.hasNext()) {
      BtreeEntry ent = c.next();
      Assert.assertNotNull(vals[((int) ent.getKey())]);
      targetSize--;
    }
    assertThat(targetSize, is(0));
  }

  @Test
  public void testMassDeletesFromBack() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    Long[] vals = new Long[100];
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 100; i++) {
        vals[i] = (long)i;
        tx.insert(i, 10 + i);
      }
      tx.commit();
      tx.close();
    }

    // delete from the back.
    for (int i = 99; i >= 0; i--) {
      WriteTx<TxDecorator> tx = bplustree.writeTx();

      tx.delete((long) i);
      if (bad(tx, i)) {
        Assert.fail();
      }
      vals[i] = null;
      tx.commit();
      tx.close();
      Tx<TxDecorator> rtx = bplustree.readTx();
      treeVerify(rtx, vals);
      rtx.close();
    }
    assertThat(bplustree.getStats().branchLeftToRight(), greaterThan(0l));
    assertThat(bplustree.getStats().branchRightToLeft(), greaterThan(0l));
    assertThat(bplustree.getStats().branchNodeMerges(), greaterThan(0l));

    store.close();
  }

  private boolean bad(WriteTx<TxDecorator> tx, Object payload) throws IOException {
    Collection<Node.VerifyError> coll = tx.verify();
    if (coll.size() > 0) {
      System.out.println("@" + payload + " " + coll);
      tx.dump(System.out);
      return true;
    }
    return false;
  }

  @Test
  public void testMassDeletesFromMiddle() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    Long[] vals = new Long[100];
    {
      WriteTx<TxDecorator> tx = bplustree.writeTx();
      for (int i = 0; i < 100; i++) {
        vals[i] = (long)i;
        tx.insert(i, 10 + i);
      }
      tx.commit();
      tx.close();
    }

    // delete from the back.
    int down = 49;
    int up = 50;

    for (; ; ) {
      if (down >= 0) {
        WriteTx<TxDecorator> tx = bplustree.writeTx();
        tx.delete((long) down);
        vals[down] = null;
        tx.commit();
        tx.close();
        Tx<TxDecorator> rtx = bplustree.readTx();
        treeVerify(rtx, vals);
        rtx.close();
      }
      if (up < 100) {
        WriteTx<TxDecorator> tx = bplustree.writeTx();
        tx.delete((long) up);
        vals[up] = null;
        tx.commit();
        tx.close();
        Tx<TxDecorator> rtx = bplustree.readTx();
        treeVerify(rtx, vals);
        rtx.close();
      }
      if (up > 99 && down < 0) {
        break;
      }
      down--;
      up++;
    }
    assertThat(bplustree.getStats().branchLeftToRight(), greaterThan(0l));
    assertThat(bplustree.getStats().branchRightToLeft(), greaterThan(0l));
    assertThat(bplustree.getStats().branchNodeMerges(), greaterThan(0l));

    store.close();
  }

  @Test
  public void testDiscard() throws IOException {
    SimpleStore store = bufferStore();
    ABPlusTree<TxDecorator> bplustree = new ABPlusTree<>(store, null, null, 10, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    WriteTx<TxDecorator> tx1 = bplustree.writeTx();
    tx1.insert(10, 10);
    tx1.commit();
    WriteTx<TxDecorator> tx2 = bplustree.writeTx();
    tx2.insert(11, 11);
    tx2.commit();

    Tx<TxDecorator> rx = bplustree.readTx();
    assertThat(rx.size(), is(2L));
    assertThat(rx.find(10l).longValue(), is(10l));
    assertThat(rx.find(11l).longValue(), is(11l));
    rx.commit();
    WriteTx<TxDecorator> tx3 = bplustree.writeTx();
    tx3.insert(12, 12);
    tx3.discard();

    rx.commit();
    assertThat(rx.size(), is(2L));
    assertThat(rx.find(12l), nullValue());

    WriteTx<TxDecorator> tx4 = bplustree.writeTx();
    tx4.insert(13, 13);
    tx4.commit();

    rx.commit();
    assertThat(rx.size(), is(3L));
    assertThat(rx.find(13l).longValue(), is(13l));

    store.close();
  }

}
