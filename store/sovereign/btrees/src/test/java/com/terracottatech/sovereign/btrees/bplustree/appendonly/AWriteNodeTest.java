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
import com.terracottatech.sovereign.btrees.bplustree.model.Node;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.btrees.bplustree.model.WriteTx;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.memory.PagedMemorySpace;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author cschanck
 */
@SuppressWarnings("unchecked")
public class AWriteNodeTest {

  private static PagedMemorySpace bufferStore() {
    return new PagedMemorySpace(PageSourceLocation.heap(), 4096, 256 * 1024, 1024*1024*1024);
  }

  @Test
  public void testEmptyLeafNode() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, true);
    Assert.assertThat(node.size(), is(0));
    Assert.assertThat(node.isEmpty(), is(true));
    Assert.assertThat(node.isLeaf(), is(true));
  }

  @Test
  public void testInsertLeafNode() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, true);
    node.insertKeyValueAt(0, 0, 0);
    node.insertKeyValueAt(1, 1, 1);
    node.insertKeyValueAt(2, 2, 2);
    Assert.assertThat(node.size(), is(3));
    for (int i = 0; i < node.size(); i++) {
      Assert.assertThat(node.getKey(i), is((long) i));
      Assert.assertThat(node.getValue(i), is((long) i));
    }
    node.insertKeyValueAt(0, 100, 100);
    Assert.assertThat(node.getKey(1), is((long) 0));
    Assert.assertThat(node.getKey(2), is((long) 1));
    Assert.assertThat(node.getKey(3), is((long) 2));
  }

  @Test
  public void testLeafNodeFull() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, true);
    for (int i = 0; i < 10; i++) {
      node.insertKeyValueAt(i, i, i);
    }
    Assert.assertThat(node.size(), is(10));
    Assert.assertThat(node.isFull(), is(true));
    for (int i = 0; i < node.size(); i++) {
      Assert.assertThat(node.getKey(i), is((long) i));
      Assert.assertThat(node.getValue(i), is((long) i));
    }
  }

  @Test
  public void testLeafNodeOverflowException() throws IOException {
    if(AMutableNode.CHECK_USAGE) {
      ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
      when(tree.getMaxKeysPerNode()).thenReturn(10);
      when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
      when(tree.getTreeStore()).thenReturn(bufferStore());

      WriteTx<TxDecorator> tx = mock(WriteTx.class);
      when(tx.getTree()).thenReturn(tree);

      AMutableNode<TxDecorator> node = new AMutableNode<>(tx, true);
      for (int i = 0; i < 10; i++) {
        node.insertKeyValueAt(i, i, i);
      }
      try {
        node.insertKeyValueAt(5, 100, 100);
        Assert.fail();
      } catch (IllegalStateException e) {
      }
    }
  }

  @Test
  public void testResizeLeafNode() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, true);
    for (int i = 0; i < 10; i++) {
      node.insertKeyValueAt(i, i, i);
    }

    node.setSize(5);
    Assert.assertThat(node.size(), is(5));
    for (int i = 0; i < node.size(); i++) {
      Assert.assertThat(node.getKey(i), is((long) i));
      Assert.assertThat(node.getValue(i), is((long) i));
    }

  }

  @Test
  public void testLeafNodeSearch() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, true);
    for (int i = 0; i < 10; i++) {
      node.insertKeyValueAt(i, 2 * i, i);
    }
    // node keys look  like 0,2,4,6,8,10,12,14,16,18
    int off = node.locate(5l);
    Assert.assertThat(~off, is(3));
    off = node.locate(8l);
    Assert.assertThat(off, is(4));
    off = node.locate(-10l);
    Assert.assertThat(~off, is(0));
    off = node.locate(100l);
    Assert.assertThat(~off, is(10));

  }

  @Test
  public void testSplitLeafNode() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    final WriteTx<TxDecorator> tx = mock(AWriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    Answer<AMutableNode<TxDecorator>> answer =new Answer<AMutableNode<TxDecorator>>() {
      @Override
      public AMutableNode<TxDecorator> answer(InvocationOnMock mock) throws Throwable {
        boolean leaf= (Boolean) mock.getArguments()[0];
        return new AMutableNode<>(tx,leaf);
      }
    };

    when(tx.createNewNode(true)).thenAnswer(answer);
    when(tx.createNewNode(false)).thenAnswer(answer);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, true);
    for (int i = 0; i < 10; i++) {
      node.insertKeyValueAt(i, i, i);
    }
    // node keys look  like 0,1,2,3,4,5,6,7,8,9
    SplitReturn split = node.split();
    Assert.assertThat(node.size(), is(5));
    Node newNode = split.getNewNode();
    Assert.assertThat(newNode.size(), is(5));
    Assert.assertThat(split.getSplitKey(), is(newNode.getKey(0)));
  }

  // branch node tests
  @Test
  public void testEmptyBranchNode() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, false);
    Assert.assertThat(node.size(), is(0));
    Assert.assertThat(node.isEmpty(), is(true));
    Assert.assertThat(node.isLeaf(), is(false));
  }

  @Test
  public void testInsertBranchNode() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, false);
    node.insertPointerKeyAt(0, 0, 0);
    node.insertPointerKeyAt(1, 1, 1);
    node.insertPointerKeyAt(2, 2, 2);
    node.setPointer(3, 4);
    Assert.assertThat(node.size(), is(3));
    for (int i = 0; i < node.size(); i++) {
      Assert.assertThat(node.getKey(i), is((long) i));
      Assert.assertThat(node.getPointer(i), is((long) i));
    }
    Assert.assertThat(node.getKey(0), is((long) 0));
    Assert.assertThat(node.getKey(1), is((long) 1));
    Assert.assertThat(node.getKey(2), is((long) 2));
    Assert.assertThat(node.getPointer(0), is((long) 0));
    Assert.assertThat(node.getPointer(3), is((long) 4));

    node.setSize(0);
    node.setPointer(0,-100l);
    node.insertKeyPointerAt(0,1,1);
    node.insertKeyPointerAt(0,2,2);
    Assert.assertThat(node.getKey(0), is((long) 2));
    Assert.assertThat(node.getKey(1), is((long) 1));
    Assert.assertThat(node.getPointer(0), is((long) -100));
    Assert.assertThat(node.getPointer(1), is((long) 2));
    Assert.assertThat(node.getPointer(2), is((long) 1));
    Assert.assertThat(node.size(), is(2));


  }

  @Test
  public void testBranchNodeFull() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, false);
    for (int i = 0; i < 10; i++) {
      node.insertPointerKeyAt(i, i, i);
    }
    Assert.assertThat(node.size(), is(10));
    Assert.assertThat(node.isFull(), is(true));
  }

  @Test
  public void testBranchNodeOverflowException() throws IOException {
    if(AMutableNode.CHECK_USAGE) {
      ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
      when(tree.getMaxKeysPerNode()).thenReturn(10);
      when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
      when(tree.getTreeStore()).thenReturn(bufferStore());
      WriteTx<TxDecorator> tx = mock(WriteTx.class);
      when(tx.getTree()).thenReturn(tree);
      AMutableNode<TxDecorator> node = new AMutableNode<>(tx, false);
      for (int i = 0; i < 10; i++) {
        node.insertPointerKeyAt(i, i, i);
      }
      try {
        node.insertPointerKeyAt(5, 100, 100);
        Assert.fail();
      } catch (IllegalStateException e) {
      }
    }
  }

  @Test
  public void testResizeBranchNode() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, false);
    for (int i = 0; i < 10; i++) {
      node.insertPointerKeyAt(i, i, i);
    }

    node.setSize(5);
    Assert.assertThat(node.size(), is(5));

  }

  @Test
  public void testBranchNodeSearch() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, false);
    for (int i = 0; i < 10; i++) {
      node.insertPointerKeyAt(i, i, 2 * i);
    }
    // node keys look  like 0,2,4,6,8,10,12,14,16,18
    int off = node.locate(5l);
    Assert.assertThat(~off, is(3));
    off = node.locate(8l);
    Assert.assertThat(off, is(4));
    off = node.locate(-10l);
    Assert.assertThat(~off, is(0));
    off = node.locate(100l);
    Assert.assertThat(~off, is(10));

  }

  @Test
  public void testSplitBranchNode() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    final WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    Answer<AMutableNode<TxDecorator>> answer =new Answer<AMutableNode<TxDecorator>>() {
      @Override
      public AMutableNode<TxDecorator> answer(InvocationOnMock mock) throws Throwable {
        boolean leaf= (Boolean) mock.getArguments()[0];
        return new AMutableNode<>(tx,leaf);
      }
    };

    when(tx.createNewNode(true)).thenAnswer(answer);
    when(tx.createNewNode(false)).thenAnswer(answer);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, false);
    for(int i=0;i<10;i++) {
      node.insertPointerKeyAt(i, i * 100, i);
    }
    // node keys look  like 0,1,2,3,4,5,6,7,8,9
    SplitReturn split = node.split();
    Assert.assertThat(node.size(),is(5));
    Node newNode = split.getNewNode();
    Assert.assertThat(newNode.size(), is(4));
    Assert.assertThat(split.getSplitKey(),is(split.getSplitKey()));
  }

  @Test
  public void testLeftRightSiblings() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    final WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    Answer<AMutableNode<TxDecorator>> answer = new Answer<AMutableNode<TxDecorator>>() {
      @Override
      public AMutableNode<TxDecorator> answer(InvocationOnMock mock) throws Throwable {
        boolean leaf = (Boolean) mock.getArguments()[0];
        return new AMutableNode<>(tx, leaf);
      }
    };
    when(tx.createNewNode(true)).thenAnswer(answer);
    when(tx.createNewNode(false)).thenAnswer(answer);

    final AMutableNode<TxDecorator> pnode = new AMutableNode<>(tx, false);
    final AMutableNode<TxDecorator> lnode1 = new AMutableNode<>(tx, true);
    final AMutableNode<TxDecorator> lnode2 = new AMutableNode<>(tx, true);

    Answer<AMutableNode<TxDecorator>> answer2 = new Answer<AMutableNode<TxDecorator>>() {
      @Override
      public AMutableNode<TxDecorator> answer(InvocationOnMock mock) throws Throwable {
        long offset = (Long) mock.getArguments()[0];
        if (offset == pnode.getOffset()) {
          return pnode;
        }
        if (offset == lnode1.getOffset()) {
          return lnode1;
        }
        if (offset == lnode2.getOffset()) {
          return lnode2;
        }
        return null;
      }
    };
    when(tx.readNode(anyLong())).thenAnswer(answer2);

    lnode1.insertKeyValueAt(0,1,1);
    lnode2.insertKeyValueAt(0, 2, 2);
    lnode1.flush();
    lnode2.flush();
    pnode.insertPointerKeyAt(0, lnode1.getOffset(), 2);
    pnode.setPointer(1, lnode2.getOffset());
    pnode.flush();

    AMutableNode<TxDecorator> rsib = lnode1.fetchRightSibling(pnode);
    Assert.assertThat(rsib,is(lnode2));
    AMutableNode<TxDecorator> lsib = lnode1.fetchLeftSibling(pnode);
    Assert.assertNull(lsib);

    rsib=lnode2.fetchRightSibling(pnode);
    Assert.assertNull(rsib);
    lsib = lnode2.fetchLeftSibling(pnode);
    Assert.assertThat(lsib,is(lnode1));
  }

  @Test
  public void testLeafNodeDeletion() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, true);
    for (int i = 0; i < 10; i++) {
      node.insertKeyValueAt(i, i, i);
    }
    node.removeKeyValueAt(5);
    Assert.assertThat(node.size(), is(9));
    Assert.assertThat(node.isFull(), is(false));
    node.removeKeyValueAt(0);
    Assert.assertThat(node.size(), is(8));
    node.removeKeyValueAt(7);
    Assert.assertThat(node.size(), is(7));
    Assert.assertThat(node.getKey(0),is(1L));
    Assert.assertThat(node.getKey(1),is(2L));
    Assert.assertThat(node.getKey(2), is(3L));
    Assert.assertThat(node.getKey(3),is(4L));
    Assert.assertThat(node.getKey(4), is(6L));
    Assert.assertThat(node.getKey(5),is(7L));
    Assert.assertThat(node.getKey(6), is(8L));
    Assert.assertThat(node.getValue(0), is(1L));
    Assert.assertThat(node.getValue(1), is(2L));
    Assert.assertThat(node.getValue(2), is(3L));
    Assert.assertThat(node.getValue(3), is(4L));
    Assert.assertThat(node.getValue(4), is(6L));
    Assert.assertThat(node.getValue(5), is(7L));
    Assert.assertThat(node.getValue(6), is(8L));
  }

  @Test
  public void testBranchNodeDelete() throws IOException {
    ABPlusTree<TxDecorator> tree = mock(ABPlusTree.class);
    when(tree.getMaxKeysPerNode()).thenReturn(10);
    when(tree.getComparator()).thenReturn(BPlusTree.DEFAULT_LONG_HANDLER);
    when(tree.getTreeStore()).thenReturn(bufferStore());

    WriteTx<TxDecorator> tx = mock(WriteTx.class);
    when(tx.getTree()).thenReturn(tree);

    AMutableNode<TxDecorator> node = new AMutableNode<>(tx, false);
    for (int i = 0; i < 10; i++) {
      node.insertPointerKeyAt(i, i, i);
    }
    node.setPointer(10,-1);
    Assert.assertThat(node.size(), is(10));
    Assert.assertThat(node.isFull(), is(true));

    node.removeKeyPointerAt(5);
    Assert.assertThat(node.size(), is(9));
    Assert.assertThat(node.isFull(), is(false));
    node.removeKeyPointerAt(0);
    Assert.assertThat(node.size(), is(8));
    node.removeKeyPointerAt(7);
    System.out.println(node);
    Assert.assertThat(node.size(), is(7));
    Assert.assertThat(node.getKey(0), is(1L));
    Assert.assertThat(node.getKey(1),is(2L));
    Assert.assertThat(node.getKey(2),is(3L));
    Assert.assertThat(node.getKey(3),is(4L));
    Assert.assertThat(node.getKey(4),is(6L));
    Assert.assertThat(node.getKey(5),is(7L));
    Assert.assertThat(node.getKey(6),is(8L));
    Assert.assertThat(node.getPointer(0),is(0L));
    Assert.assertThat(node.getPointer(1),is(2L));
    Assert.assertThat(node.getPointer(2),is(3L));
    Assert.assertThat(node.getPointer(3),is(4L));
    Assert.assertThat(node.getPointer(4),is(5L));
    Assert.assertThat(node.getPointer(5),is(7L));
    Assert.assertThat(node.getPointer(6),is(8L));
    Assert.assertThat(node.getPointer(7),is(9L));
  }
}
