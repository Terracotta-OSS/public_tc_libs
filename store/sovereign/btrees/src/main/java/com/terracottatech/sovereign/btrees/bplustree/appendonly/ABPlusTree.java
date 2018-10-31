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

import com.terracottatech.sovereign.btrees.bplustree.AccessManager;
import com.terracottatech.sovereign.btrees.bplustree.CommitType;
import com.terracottatech.sovereign.btrees.bplustree.Finger;
import com.terracottatech.sovereign.btrees.bplustree.TreeLocation;
import com.terracottatech.sovereign.btrees.bplustree.model.BPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;
import com.terracottatech.sovereign.btrees.bplustree.model.IngestHandle;
import com.terracottatech.sovereign.btrees.bplustree.model.KeyValueHandler;
import com.terracottatech.sovereign.btrees.bplustree.model.MutableNode;
import com.terracottatech.sovereign.btrees.bplustree.model.Node;
import com.terracottatech.sovereign.btrees.bplustree.model.ReadTx;
import com.terracottatech.sovereign.btrees.bplustree.model.Tx;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecoratorFactory;
import com.terracottatech.sovereign.btrees.bplustree.model.WriteTx;
import com.terracottatech.sovereign.btrees.stores.SimpleStore;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of a B+Tree, based on a garbage collecting appending store.
 * Key/Value's must be longs. Secondary data stores are supports to manage commit boundaries.
 * <p>No node can be changed in place, rather it must be written to a new location. So, for every change,
 * the node must be changed, then its parent must be updated. This seems heavy, but allows for
 * lovely lock free reads with a single writer.</p>
 * <p>It uses a fixed set of active transactions lots to capture which revisions of the tree
 * are active at any time. This allows for the garbage collection of freed locations based on the
 * revision they were freed in. This also means that allocations and frees are very bursty. It also means
 * that transactions *must* be closed/committed when they are no longer useful.</p>
 * <p>A smattering of introductory reading material I used to build this...</p>
 * <table>
 * <tr>
 * <td><a href="http://en.wikipedia.org/wiki/B-tree">Wikipedia</a>
 * </tr>
 * <tr>
 * <td><a href="http://www.cs.utexas.edu/users/djimenez/utsa/cs3343/lecture16.html">Intro stuff</a>
 * </tr>
 * <tr>
 * <td><a href="http://www.cs.utexas.edu/users/djimenez/utsa/cs3343/lecture17.html">More Intro stuff</a>
 * </tr>
 * <tr>
 * <td><a href="http://www.bzero.se/ldapd/btree.html">Appending btree visual explanation</a>
 * </tr>
 * <tr>
 * <td><a href="http://www.cs.uofs.edu/~mccloske/courses/cmps340/btree_alg.html">More insert/delete stuff</a>
 * </tr>
 * </table>
 *
 * @author cschanck
 */
public class ABPlusTree<W extends TxDecorator> implements BPlusTree<W> {

  public static final int DEFAULT_NUMBER_OF_READ_SLOTS = Math.max(64, Runtime.getRuntime().availableProcessors() * 16);
  public static final int DEFAULT_GC_MINIMUM = Math.max(1, Integer.getInteger("sovereign.btree.gcmin", 100));
  public static final int DEFAULT_TX_CACHE_SIZE = 128;

  private final TimerTask flushTask = new TimerTask() {
    @Override
    public void run() {
      if (modified && !durableCommitPending) {
        durableCommitPending = true;
      }
    }
  };

  private final TxDecoratorFactory<W> decoratorFactory;

  private enum InsertOps {

    PUT,

    INSERT_IF_ABSENT,

    REPLACE_ANY,

    REPLACE_SPECIFIC
  }

  private final SimpleStore treeStore;
  private final int maxKeysPernode;
  private final int maxBytesPerNode;
  private final KeyValueHandler<W> comparator;
  private final ReentrantLock treeLock = new ReentrantLock();
  private final Timer timer;
  private final AccessManager gc;
  private volatile boolean durableCommitPending = false;
  private volatile boolean modified = false;
  private volatile SnapShot snapShot = null;
  private final StatsImpl stats = new StatsImpl();

  /**
   * Instantiates a new Appending B+tree.
   *
   * @param treeStore tree store
   * @param dataStores up to two data stores.
   * @param maxKeysPerNode maximum number of keys per node.
   * @param comparator comparator
   * @param asyncFlushInMS time to flush durably in ms. &lt;=0 will ignore.
   * @param gcMinimumCount minimum freed threshold for store gc to occur.
   * @throws IOException
   */
  public ABPlusTree(SimpleStore treeStore, SimpleStore[] dataStores, TxDecoratorFactory<W> decoratorFactory,
                    int maxKeysPerNode, KeyValueHandler<W> comparator, int asyncFlushInMS,
                    int gcMinimumCount) throws IOException {
    this.treeStore = treeStore;
    if (decoratorFactory == null) {
      this.decoratorFactory = new TxDecoratorFactory.None<W>();
    } else {
      this.decoratorFactory = decoratorFactory;
    }
    GarbageCollectionQueue.GCTask task = op -> op.getStore().storeFree(op.getAddress(), false);

    this.gc = new AccessManager(task, (rev) -> {
      notifyOfGC(rev);
    }, gcMinimumCount);
    this.maxKeysPernode = maxKeysPerNode;
    this.maxBytesPerNode = ANode.maxSizeFor(maxKeysPerNode);
    this.comparator = comparator;
    treeStore.getStoreLocation().ensure();
    reopen();
    if (asyncFlushInMS > 0 && treeStore.supportsDurability()) {
      timer = new Timer("Btree ASync Flush", true);
      timer.scheduleAtFixedRate(flushTask, asyncFlushInMS, asyncFlushInMS);
    } else {
      timer = null;
    }
  }

  /**
   * Instantiates a new Appending B+tree. Default number of read slots, gc minimum, and tx cache size.
   *
   * @param treeStore the tree store
   * @param dataStores the data stores
   * @param maxKeysPerNode the max keys per node
   * @param comparator the comparator
   * @param asyncFlushInMS the async flush in mS
   * @throws IOException the iO exception
   */
  public ABPlusTree(SimpleStore treeStore, SimpleStore[] dataStores, TxDecoratorFactory<W> factory, int maxKeysPerNode,
                    KeyValueHandler<W> comparator, int asyncFlushInMS) throws IOException {
    this(treeStore, dataStores, factory, maxKeysPerNode, comparator, asyncFlushInMS, DEFAULT_GC_MINIMUM);
  }

  /**
   * Gets the access manager for this tree.
   *
   * @return the gC
   */
  public AccessManager getAccessManager() {
    return gc;
  }

  public boolean isDurableCommitPending() {
    return durableCommitPending;
  }

  private void reopen() throws IOException {
    snapShot = new SnapShot(-1L, 0, null, 0, 1);
    if (snapShot.getRootAddress() < 0) {
      initNewTree();
    }
  }

  void readSnapShot() throws IOException {
    ByteBuffer b = ByteBuffer.allocate(3 * 8 + 4);
    try {
      getTreeStore().getCommitData(b);
      if (b.remaining() == 0) {
        b.clear();
        long root = b.getLong(0);
        long size = b.getLong(8);
        long rev = b.getLong(16);
        int ht = b.getInt(24);
        gc.setCurrentRevisionNumber(rev);
        snapShot = new SnapShot(root, rev, b, size, ht);
      } else {
        gc.setCurrentRevisionNumber(0);
        snapShot = new SnapShot(-1L, 0L, b, 0L, 0);
      }
    } catch (IOException e) {
      snapShot = new SnapShot(-1L, 0L, b, 0L, 0);
    }
  }

  /**
   * Gets tree store.
   *
   * @return the tree store
   */
  @Override
  public SimpleStore getTreeStore() {
    return treeStore;
  }

  /**
   * Gets max keys per node.
   *
   * @return the max keys per node
   */
  @Override
  public int getMaxKeysPerNode() {
    return maxKeysPernode;
  }

  /**
   * Gets comparator.
   *
   * @return the comparator
   */
  @Override
  public KeyValueHandler<W> getComparator() {
    return comparator;
  }

  /**
   * Gets snap shot.
   *
   * @return the snap shot
   */
  @Override
  public SnapShot getSnapShot() {
    return snapShot;
  }

  @Override
  public BtreeEntry delete(WriteTx<W> tx, long key) throws IOException {
    return rawDelete(tx, key, 0, false);
  }

  @Override
  public BtreeEntry delete(WriteTx<W> tx, long keyObject, long valueObject) throws IOException {
    return rawDelete(tx, keyObject, valueObject, true);
  }

  @Override
  public StatsImpl getStats() {
    return stats;
  }

  /*
   * This is the non-recursive entry point for deletes. Similarly to the insert path, this calls
   * the workhorse recursive delete, then is responsible for root node fixups.
   */
  private BtreeEntry rawDelete(WriteTx<W> tx, long keyObject, long valueObject, boolean matchValue) throws IOException {
    modified = true;
    stats.recordDelete();
    MutableNode rootNode = tx.readNode(tx.getWorkingRootOffset());
    DeleteReturn ret = deleteFrom(tx, null, -1, rootNode, keyObject, valueObject, matchValue);
    if (ret.isNodeChanged()) {
      if (rootNode.isEmpty()) {
        if (rootNode.isLeaf()) {
          tx.setWorkingRootOffset(rootNode.getOffset());
        } else {
          tx.setWorkingRootOffset(rootNode.getPointer(0));
          tx.queueFreeNode(rootNode.getOffset(), rootNode.getRevision());
          tx.recordHeightDelta(-1);
        }
      } else {
        tx.setWorkingRootOffset(rootNode.getOffset());
      }
      return ret;
    } else {
      return null;
    }
  }

  /*
   * This is the recursive delete function. It descend the tree until a leaf node is found, then,
   * assuming the deletion actually occurs (key is present, etc), it deletes the entry fromt he leaf. Note
   * that this can temporarily invalidate the tree structure by making a node that is too small; this
   * is ok, only this tx can see this version of the tree. If an invalid tree is produced, the rebalancing
   * method will be called.
   *
   * In either case, you need to write the children out and then write parents out after children
   * because of the append only nature of the store.
   */
  private DeleteReturn deleteFrom(WriteTx<W> tx, MutableNode parent, int parentPosition, MutableNode node, long key,
                                  long value, boolean matchValue) throws IOException {
    DeleteReturn ret;
    if (node.isLeaf()) {
      int point = node.locate(key);
      if (point < 0) {
        return new DeleteReturn();
      } else if (matchValue) {
        if (value != node.getValue(point)) {
          // needed to match value too, didn't, so bail.
          return new DeleteReturn();
        }
      }

      // all good, make the change
      ret = new DeleteReturn(node.getKey(point), node.getValue(point));
      node.removeKeyValueAt(point);
      // flush the node.
      node.flush();
      tx.recordSizeDelta(-1);
      if (parent != null) {
        // if we have a parent, set the key in the parent position. mod the parent and
        // flush it. I think this is a bit sloppy, as it could be better done on the return.
        // but several algorithms insisted ont his approach, and it works.
        parent.setPointer(parentPosition + 1, node.getOffset());
        if (point == 0) {
          if (parentPosition >= 0) {
            parent.setKey(parentPosition, node.smallestKeyUnder());
          }
        }
        parent.flush();
      }
    } else {
      int point = node.locate(key);
      if (point < 0) {
        point = ~point;
      } else {
        point++;
      }
      MutableNode nextNode = tx.readNode(node.getPointer(point));
      // delete somewhere below us.
      ret = deleteFrom(tx, node, point - 1, nextNode, key, value, matchValue);
      if (ret.isNodeChanged()) {
        if (parent != null) {
          // again, mod the parent. seems backwards.
          parent.setPointer(parentPosition + 1, node.getOffset());
          if (point == 0) {
            if (parentPosition >= 0) {
              parent.setKey(parentPosition, node.smallestKeyUnder());
            }
          }
          parent.flush();
        }
      }
    }
    if (parent != null && node.size() < node.minSize()) {
      // oh, noes! there is work to do. the node is too damn small.
      adjustPostDeletion(tx, parent, parentPosition, node);
    }
    return ret;
  }

  /*
    Rebalancing Notes
      Concatenate(C-D): Branch Nodes

          +-----+-+-+-+-----+             +-----+-+-+-----+
        A | ... |u|w|y| ... |           A | ... |u|y| ... |
          +-----+-+-+-+-----+             +-----+-+-+-----+
                | | | |                         | | |
                | | | |                         | | |
                B | | Z        ======>          B | Z
                  / \                             |
                 /   \                            |
        +-----+-+     +-+-----+           +----+-+-+-+----+
      C | ... |v|     |x| ... | D      C' | .. |v|w|x| .. |
        +-----+-+     +-+-----+           +----+-+-+-+----+
                |     |                          | |
                |     |                          | |
                E     F                          E F

        insert parent key at end of leftsib
        insert first rightsib pointer at end of leftsib
        move pointer/key pairs from right sib to end of left sib
        leftsib flush
        update parent before to leftsib
        delete parent key and next pointer
        parent flush
        rightsib free

      Concatenate(C-D): Leaf Nodes

          +-----+-+-+-+-----+             +-----+-+-+-----+
        A | ... |u|x|y| ... |           A | ... |u|y| ... |
          +-----+-+-+-+-----+             +-----+-+-+-----+
                | | | |                         | | |
                | | | |                         | | |
                B | | Z         ======>         B | Z
                  / \                             |
                 /   \                            |
        +-----+-+     +-+-----+           +----+-+-+-+----+
      C |u|...|v|     |x| ... | D      C' |u|.. |v|x|  .. |
        +-----+-+     +-+-----+           +----+-+-+-+----+

        append all keys and values from right sib to left sib
        flush leftsib
        update parent before pointer to leftsib
        remove key/pointer in parent
        flush parent
        free rightsib

      Right/Left-Redistribute(C) Branch nodes
           +-----+-+-+-+-----+                    +-----+-+-+-+-----+
         A | ... |u|w|z| ... |                  A | ... |u|x|z| ... |
           +-----+-+-+-+-----+                    +-----+-+-+-+-----+
                   / \                                   /  \
                  /   \             ======>             /    \
                 /     \                               /      \
          +------+-+   +-+-+-----+            +------+-+-+   +-+------+
        B | .... |v| C |x|y| ... |          B | .... |v|w| C |y| .... |
          +------+-+   +-+-+-----+            +------+-+-+   +-+------+
                 | |   | | |                         | | |   | |
                 | |   | | |                         | | |   | |
                 | |   | | |                         | | |   | |
                 D E   F G H                         D E F   G H

          append parent key to leftsib
          append rightsib first pointer to leftsib
          leftsib flush
          remove first pointer/key from rightsib
          rightsib flush
          set parent key to rightsib minkey
          set parent before pointer to leftsib
          set parent after pointer to rightsib
          flush parent


      Right/Left-Redistribute(C): Leaf Nodes
           +-----+-+-+-+-----+                    +-----+-+-+-+-----+
         A | ... |u|x|z| ... |                  A | ... |u|y|z| ... |
           +-----+-+-+-+-----+                    +-----+-+-+-+-----+
                   / \                                   /  \
                  /   \             ======>             /    \
                 /     \                               /      \
          +------+-+   +-+-+-----+            +------+-+-+   +-+------+
        B | .... |v| C |x|y| ... |          B'| .... |v|x| C'|y| .... |
          +------+-+   +-+-+-----+            +------+-+-+   +-+------+

         move rightsib first key/val to end of leftsib
         remove rightsib first key/val
         flush leftsib
         flush rightsib
         set parent key to first of rightsib
         set parent before pointer to leftsib
         set parent after pointer to rightsib
         flush parent

     Left/Right rebalancing B->C

     Example:
        LR-Redistribute - Branch Nodes(B):
               +-----+-+-+-+-----+                  +-----+-+-+-+-----+
             A | ... |u|x|z| ... |                A | ... |u|w|z| ... |
               +-----+-+-+-+-----+                  +-----+-+-+-+-----+
                      /  \                                  / \
                     /    \                                /   \
                    /      \                              /     \
           +------+-+-+   +-+------+    ======>    +------+-+   +-+-+-----+
         B | .... |v|w| C |y| .... |             B'| .... |v| C'|x|y| ... |
           +------+-+-+   +-+------+               +------+-+   +-+-+-----+
                  | | |   | |                             | |   | | |
                  | | |   | |                             | |   | | |
                  | | |   | |                             | |   | | |
                  D E F   G H                             D E   F G H

         insert parent key down to 0 key entry of rightsib,
         rightsib all pointers move up 1
         letsib last pointer becomes pointer 0 of rightsib
         last leftsib minkey becomes new parent key
         leftsib crop by 1
         flush leftsib
         flush rightsib
         parent pointer before becomes leftsib
         parent pointer after becomes rightsib

        Left/Right-Rebalancing
               +-----+-+-+-+-----+                  +-----+-+-+-+-----+
             A | ... |u|x|z| ... |                A | ... |u|w|z| ... |
               +-----+-+-+-+-----+                  +-----+-+-+-+-----+
                      /  \                                  / \
                     /    \                                /   \
                    /      \                              /     \
           +------+-+-+   +-+------+    ======>    +------+-+   +-+-+-----+
         B | .... |v|w| C |x| .... |             B | .... |v| C |w|x| ... |
           +------+-+-+   +-+------+               +------+-+   +-+-+-----+

         insert leftsib last key,val to first key,val entry of rightsib,
         crop last entry on leftsib
         change parent key to new rightsib first key
         flush leftsib
         flush rightsib
         parent pointer before becomes leftsib
         parent pointer after becomes rightsib
         flush parent

   */

  /*
   * Here the node is too small; we have to fix that. There are 4 ways to do this. You can
   * steal some entries from the left or right siblings (if that won't make a sibling too small)
   * or you can collapse this node together with it's left or right siblings. Either one of the
   * two pairs can be possible, not both, due to sizing constraints.
   *
   * Of course, after this fixup, the parent must be fixed...
   */

  private void adjustPostDeletion(WriteTx<W> tx, MutableNode parent, int parentPosition,
                                  MutableNode node) throws IOException {

    // steal from left sib if it exists and has enough entries.
    MutableNode leftSib = node.fetchLeftSibling(parent);
    if (leftSib != null && balanceLeftToRight(parent, parentPosition, leftSib, node)) {
      return;
    }

    MutableNode rightSib = node.fetchRightSibling(parent);
    if (leftSib == null && rightSib == null) {
      // bail, no help;
      return;
    }

    // steal from right sib if it exists and has enough entries.
    if (balanceRightToLeft(parent, parentPosition + 1, node, rightSib)) {
      return;
    }

    // push this nodes remaining data into the right sib, if it exists
    if (mergeIntoLeftNode(tx, parent, parentPosition, leftSib, node)) {
      return;
    }

    // push the rightsib (if it exists) data into this node
    if (mergeIntoLeftNode(tx, parent, parentPosition + 1, node, rightSib)) {
      return;
    }
    // this. this is a problem. should never happen.
    throw new IllegalStateException();

  }

  // steal from the left sibling
  private boolean balanceLeftToRight(MutableNode parent, int parentPosition, MutableNode left,
                                     MutableNode right) throws IOException {
    if (left != null && left.size() > left.minSize() && left.isLeaf() == right.isLeaf()) {
      if (left.isLeaf()) {
        // leaf case

        // insert leftsib last key,val to first key,val entry of rightsib,
        long k = left.getKey(left.size() - 1);
        long v = left.getValue(left.size() - 1);
        right.insertKeyValueAt(0, k, v);

        // crop last off of left.
        left.setSize(left.size() - 1);

        // flush leftsib, flush rightsib
        left.flush();
        right.flush();

        // change parent key to new rightsib first key
        parent.setKey(parentPosition, right.smallestKeyUnder());
        // parent pointer before becomes leftsib
        parent.setPointer(parentPosition, left.getOffset());
        // parent pointer after becomes rightsib
        parent.setPointer(parentPosition + 1, right.getOffset());

        // flush parent
        parent.flush();

        stats.recordLeafLeftToRight();
        return true;
      } else {
        // branch case

        // insert parent key down to 0 key entry of rightsib,
        // rightsib all pointers move up 1
        // letsib last pointer becomes pointer 0 of rightsib
        long pk = parent.getKey(parentPosition);
        long np = left.getPointer(left.size());
        right.insertPointerKeyAt(0, np, pk);

        // leftsib crop by 1
        left.setSize(left.size() - 1);

        // flush leftsib
        // flush rightsib
        right.flush();
        left.flush();

        // last leftsib minkey becomes new parent key
        parent.setKey(parentPosition, right.smallestKeyUnder());

        //parent pointer before becomes leftsib
        parent.setPointer(parentPosition, left.getOffset());

        //parent pointer after becomes rightsib
        parent.setPointer(parentPosition + 1, right.getOffset());

        // flush parent
        parent.flush();

        stats.recordBranchleftToRight();
        return true;
      }
    }
    return false;
  }

  // steal from the right sibling
  private boolean balanceRightToLeft(MutableNode parent, int parentPosition, MutableNode left,
                                     MutableNode right) throws IOException {
    if (right != null && right.size() > right.minSize() && left.isLeaf() == right.isLeaf()) {
      if (left.isLeaf()) {
        // leaf case

        //move rightsib first key/val to end of leftsib
        long k = right.getKey(0);
        long v = right.getValue(0);
        left.insertKeyValueAt(left.size(), k, v);

        // delete first key value in right
        right.removeKeyValueAt(0);

        //flush leftsib flush rightsib
        left.flush();
        right.flush();

        // change parent key to new rightsib first key
        parent.setKey(parentPosition, right.smallestKeyUnder());
        // parent pointer before becomes leftsib
        parent.setPointer(parentPosition, left.getOffset());
        // parent pointer after becomes rightsib
        parent.setPointer(parentPosition + 1, right.getOffset());
        //flush parent
        parent.flush();

        stats.recordLeafRightToLeft();
        return true;
      } else {
        // branch case

        // append parent key to leftsib
        // append rightsib first pointer to leftsib
        long lk = parent.getKey(parentPosition);
        long lp = right.getPointer(0);
        left.insertKeyPointerAt(left.size(), lk, lp);

        // leftsib flush
        left.flush();

        // remove first pointer/key from rightsib
        right.removePointerKeyAt(0);

        // rightsib flush
        right.flush();

        // set parent key to rightsib minkey
        parent.setKey(parentPosition, right.smallestKeyUnder());

        // set parent before pointer to leftsib
        parent.setPointer(parentPosition, left.getOffset());

        // set parent after pointer to rightsib
        parent.setPointer(parentPosition + 1, right.getOffset());

        // flush parent
        parent.flush();

        stats.recordBranchRightToLeft();
        return true;
      }
    }
    return false;
  }

  // merge into the left node.
  private boolean mergeIntoLeftNode(WriteTx<W> tx, MutableNode parent, int parentPosition, MutableNode left,
                                    MutableNode right) throws IOException {
    if (left != null && (left.size() + right.size()) < tx.getTree().getMaxKeysPerNode() && left.isLeaf() == right.isLeaf()) {
      if (left.isLeaf()) {
        // leaf case

        // append all keys and values from right sib to left sib
        for (int i = 0; i < right.size(); i++) {
          int next = left.size();
          left.setKey(next, right.getKey(i));
          left.setValue(next, right.getValue(i));
        }

        // flush leftsib
        left.flush();

        // update parent before pointer to leftsib
        parent.setPointer(parentPosition, left.getOffset());

        // remove key/pointer in parent
        parent.removeKeyPointerAt(parentPosition);

        // flush parent
        parent.flush();

        // free rightsib
        tx.queueFreeNode(right.getOffset(), right.getRevision());

        stats.recordLeafNodeMerges();
        return true;
      } else {
        // branch case

        // insert parent key at end of leftsib
        // insert first rightsib pointer at end of leftsib
        long pk = parent.getKey(parentPosition);
        long sp = right.getPointer(0);

        left.insertKeyPointerAt(left.size(), pk, sp);

        // move key/pointer pairs from right sib to end of left sib
        for (int i = 0; i < right.size(); i++) {
          int lsize = left.size();
          long k = right.getKey(i);
          long p = right.getPointer(i + 1);
          left.setKey(lsize, k);
          left.setPointer(lsize + 1, p);
        }

        // leftsib flush
        left.flush();

        // update parent before to leftsib
        parent.setPointer(parentPosition, left.getOffset());

        // delete parent key and next pointer
        parent.removeKeyPointerAt(parentPosition);

        // parent flush
        parent.flush();

        // rightsib free
        tx.queueFreeNode(right.getOffset(), right.getRevision());

        stats.recordBranchNodeMerges();
        return true;
      }
    }
    return false;
  }

  void updateSnapShot(SnapShot ss) {
    this.snapShot = ss;
  }

  /*
   * This is base tree commit. Commits a tx's changes to be public for other tx's. It
   * is also responsible for committing the extra stores, to keep things in lockstep.
   * Note that they are committed first...
   */
  @Override
  public void commit(WriteTx<W> treeTx, CommitType commitType) throws IOException {
    long newSize = snapShot.getSize() + treeTx.getSizeDelta();
    int newHeight = snapShot.getHeight() + treeTx.getHeightDelta();

    /*
     * TODO this should use the dumbstruct stuff, because it would be much simpler.
     */
    ByteBuffer b = ByteBuffer.allocate(3 * 8 + 4);
    b.putLong(0, treeTx.getWorkingRootOffset());
    b.putLong(8, newSize);
    b.putLong(16, treeTx.getWorkingRevision());
    b.putInt(24, newHeight);
    if (isDurableCommitPending() || !treeStore.supportsDurability()) {
      commitType = CommitType.DURABLE;
    }
    switch (commitType) {
      case VISIBLE:
        treeStore.commit(false, b);
        break;
      case DURABLE:
        treeStore.commit(true, b);
        modified = false;
        durableCommitPending = false;
        break;
      default:
        throw new IllegalStateException();
    }
    updateSnapShot(new SnapShot(treeTx.getWorkingRootOffset(), treeTx.getWorkingRevision(), b, newSize, newHeight));
    stats.recordCommit();
  }

  private void initNewTree() throws IOException {
    WriteTx<W> tx = writeTx();
    try {
      tx.begin();
      MutableNode node = tx.createNewNode(true);
      tx.setWorkingRootOffset(node.flush());
      tx.commit(CommitType.DURABLE);
    } finally {
      try {
        tx.discard();
      } finally {
        tx.close();
      }
    }
  }

  public void garbageCollect() throws IOException {
    gc.garbageCollect(snapShot.getRevision());
  }

  public void queueFree(SimpleStore s, long workingRevision, long offset) {
    gc.queueFree(s, workingRevision, offset);
  }

  public void invalidateRevision(long workingRevision) {
    gc.invalidateRevision(workingRevision);
  }

  @Override
  public long size(Tx<W> treeTx) {
    if (treeTx instanceof WriteTx) {
      return snapShot.getSize() + ((WriteTx) treeTx).getSizeDelta();
    } else {
      return snapShot.getSize();
    }
  }

  @Override
  public int height(Tx<W> treeTx) {
    if (treeTx instanceof WriteTx) {
      return snapShot.getHeight() + ((WriteTx) treeTx).getHeightDelta();
    } else {
      return snapShot.getHeight();
    }
  }

  @Override
  public BtreeEntry insert(WriteTx<W> tx, long key, long value) throws IOException {
    return rawInsert(tx, key, value, null, InsertOps.PUT);
  }

  @Override
  public BtreeEntry insertIfAbsent(WriteTx<W> tx, long key, long value) throws IOException {
    return rawInsert(tx, key, value, null, InsertOps.INSERT_IF_ABSENT);
  }

  @Override
  public BtreeEntry replace(WriteTx<W> tx, long key, long newValue) throws IOException {
    return rawInsert(tx, key, newValue, null, InsertOps.REPLACE_ANY);
  }

  @Override
  public BtreeEntry replace(WriteTx<W> tx, long key, long expectedValue, long newValue) throws IOException {
    return rawInsert(tx, key, newValue, expectedValue, InsertOps.REPLACE_SPECIFIC);
  }

  /*
   * OK, this is the the top (non recursive) of the insertion process. Basically, after the recursive
   * work is done, it has to handle the root node changes. It might grow the tree, it might just
   * repoint the root node.
   */
  private BtreeEntry rawInsert(WriteTx<W> tx, long key, long value, Long possibleExpectedValue,
                               InsertOps operation) throws IOException {
    modified = true;
    stats.recordInsert();
    MutableNode rootNode = tx.readNode(tx.getWorkingRootOffset());
    InsertReturn ret = insertInto((AWriteTx<W>) tx, rootNode, key, value, possibleExpectedValue, operation);
    if (ret.isNodeChanged()) {
      // Every time, the root node had to change, unless it was a conditional insert
      // or some such. Did it change merely because the
      // pointer had to be updated, or does a new root need to be created.
      if (!ret.isBubbled()) {
        // here we just repoint the root node.
        tx.setWorkingRootOffset(rootNode.getOffset());
      } else {
        // here we bubbled a key up from the root node. This means we grow the tree.
        MutableNode newNode = tx.createNewNode(false);
        newNode.setPointer(0, ret.getBubblePrevNode().getOffset());
        newNode.setPointer(1, ret.getBubbleNextNode().getOffset());
        newNode.setKey(0, ret.getBubbleKey());
        tx.setWorkingRootOffset(newNode.flush());
        tx.recordHeightDelta(1);
      }
    }
    if (ret.getInvolvedValue() != null) {
      return new BtreeEntry(key, ret.getInvolvedValue());
    } else {
      return null;
    }
  }

  /*
   * This is the recursive insertion function. It attempts to insert the key/value into the
   * specified node (modulo which insert operation it is). If this node is a leaf node, the
   * insertion could proceed. If the node is made full by this insertion, it is split into
   * two keys, and the split key is "bubbled up" tot he parent for handling.
   *
   * If it is a branch node, the appropriate child node is found and then this method is invoked
   * for that node. Upon return, the branch node may need to respond tot a splitting event.
   * If the child node was split, a new key/pointer pair needs to be inserted in the branch node.
   * This of course might lead to another full node, etc, all the way up the tree.
   *
   * The modifications are done in this order on purpose, because of the underlying appending nature
   * of the stores. Each change of a node implies a write to a new location, hence the address
   * will change. So children must be written before parents.
   *
   * On the other hand, this means no locking at all for reads while a write is going on.
   *
   */
  private InsertReturn insertInto(WriteTx<W> tx, MutableNode node, long key, long value, Long possibleExpectedValue,
                                  InsertOps operation) throws IOException {
    if (node.isLeaf()) {

      int insertionPoint = node.locate(key);
      InsertReturn ret = new InsertReturn();
      boolean found = true;

      if (insertionPoint < 0) {
        found = false;
        insertionPoint = ~insertionPoint;
      }

      if (found) {
        switch (operation) {
          case INSERT_IF_ABSENT:
            ret.setValueInvolved(node.getValue(insertionPoint));
            return ret;
          case REPLACE_ANY:
          case PUT:
            ret.setValueInvolved(node.getValue(insertionPoint));
            ret.setKeyRemoved(node.getKey(insertionPoint));
            node.setKey(insertionPoint, key);
            node.setValue(insertionPoint, value);
            break;
          case REPLACE_SPECIFIC:
            long tmp = node.getValue(insertionPoint);
            if (tx.getTree().getComparator().compareObjectKeys(tx, possibleExpectedValue, tmp) != 0) {
              ret.setValueInvolved(tmp);
              return ret;
            }
            ret.setValueInvolved(node.getValue(insertionPoint));
            ret.setKeyRemoved(node.getKey(insertionPoint));
            node.setKey(insertionPoint, key);
            node.setValue(insertionPoint, value);
            break;
          default:
            throw new IllegalStateException(operation.toString());
        }
      } else {
        switch (operation) {
          case REPLACE_ANY:
          case REPLACE_SPECIFIC:
            return ret;
          case INSERT_IF_ABSENT:
          case PUT:
            break;
          default:
            throw new IllegalStateException(operation.toString());
        }

        node.insertKeyValueAt(insertionPoint, key, value);
        tx.recordSizeDelta(1);
      }

      ret.setNodeChanged(true);

      if (node.isFull()) {
        SplitReturn sr = node.split();
        ret.setBubbled(true).setBubbleNextNode(sr.getNewNode()).setBubbleKey(
          sr.getNewNode().getKey(0)).setBubblePrevNode(node);
        stats.recordLeafNodeSplit();
        return ret;
      } else {
        ret.setBubbled(false);
        node.flush();
        return ret;
      }

    } else {

      int traversalPoint = node.locate(key);
      if (traversalPoint < 0) {
        traversalPoint = ~traversalPoint;
      } else {
        traversalPoint++;
      }
      MutableNode nextNode = tx.readNode(node.getPointer(traversalPoint));

      InsertReturn ret = insertInto(tx, nextNode, key, value, possibleExpectedValue, operation);

      if (ret.isBubbled()) {

        int insertionPoint = traversalPoint;

        node.insertPointerKeyAt(insertionPoint, ret.getBubblePrevNode().getOffset(), ret.getBubbleKey());
        node.setPointer(insertionPoint + 1, ret.getBubbleNextNode().getOffset());
        ret.setBubbled(false).setBubblePrevNode(null).setBubbleNextNode(null);

        if (node.isFull()) {
          SplitReturn sr = node.split();
          Node newNode = sr.getNewNode();
          ret.setBubbled(true).setBubbleNextNode(newNode).setBubbleKey(sr.getSplitKey()).setBubblePrevNode(node);
          stats.recordBranchNodeSplit();
        } else {
          node.flush();
        }
        ret.setNodeChanged(true);
      } else {
        if (ret.isNodeChanged()) {
          node.setPointer(traversalPoint, nextNode.getOffset());
          node.flush();
        } else {
          ret.setNodeChanged(false);
        }
      }
      return ret;
    }
  }

  /*
   * Top level non-recursive search method.
   */
  @Override
  public Finger searchForNode(Tx<W> tx, Object key) throws IOException {
    Finger f = new Finger();
    Node node = tx.readNode(tx.getWorkingRootOffset());
    rawSearch(tx, key, node, f);
    stats.recordGet();
    return f;
  }

  @Override
  public Finger scanForNode(Tx<W> tx, long start) throws IOException {
    Finger f = new Finger();
    Node node = tx.readNode(tx.getWorkingRootOffset());
    rawScan(tx, start, node, f);
    stats.recordGet();
    return f;
  }

  /**
   * Recursive search method. Walks down the tree, using the node locate method.
   *
   * @param tx transaction
   * @param key key
   * @param node current node
   * @param finger list of visited parent nodes. Populated as we go.
   * @throws IOException
   */
  public void rawSearch(Tx<W> tx, Object key, Node node, Finger finger) throws IOException {
    if (node.isLeaf()) {
      int index = node.locate(key);
      TreeLocation loc = new TreeLocation(node, index);
      finger.add(loc);
      return;
    }

    int index = node.locate(key);
    if (index < 0) {
      index = ~index;
    } else {
      index++;
    }

    TreeLocation loc = new TreeLocation(node, index);
    finger.add(loc);
    rawSearch(tx, key, tx.readNode(node.getPointer(index)), finger);
  }

  /**
   * Recursive search method. Walks down the tree, using the node locate method.
   *
   * @param tx transaction
   * @param key key
   * @param node current node
   * @param finger list of visited parent nodes. Populated as we go.
   * @throws IOException
   */
  public void rawScan(Tx<W> tx, long key, Node node, Finger finger) throws IOException {
    if (node.isLeaf()) {
      int index = node.locate(key);
      TreeLocation loc = new TreeLocation(node, index);
      finger.add(loc);
      return;
    }

    int index = node.locate(key);
    if (index < 0) {
      index = ~index;
    } else {
      index++;
    }

    TreeLocation loc = new TreeLocation(node, index);
    finger.add(loc);
    rawScan(tx, key, tx.readNode(node.getPointer(index)), finger);
  }

  @Override
  public void close() throws IOException {
    getTreeStore().close();
    if (timer != null) {
      timer.cancel();
    }
  }

  public void dump(PrintStream pw) throws IOException {
    Tx<W> tx = readTx();
    tx.begin();
    try {
      dump(tx, pw);
    } finally {
      tx.close();
    }
  }

  public void dump(Tx<W> tx, PrintStream pw) throws IOException {
    tx.begin();
    pw.println("Tree: @" + tx.getWorkingRootOffset());
    Node rootNode = tx.readNode(tx.getWorkingRootOffset());
    AtomicInteger leafNodeCounter = new AtomicInteger(0);
    AtomicInteger branchNodeCounter = new AtomicInteger(0);
    dumpFromNode(tx, pw, rootNode, 0, branchNodeCounter, leafNodeCounter);
    pw.println("Tree: " + (leafNodeCounter.get() + branchNodeCounter.get()) + " nodes (" +
                 branchNodeCounter.get() + "B/" + leafNodeCounter.get() + "L). Size: " + tx.size());
    pw.flush();
  }

  private void dumpFromNode(Tx<W> tx, PrintStream pw, Node node, int level, AtomicInteger branchNodeCounter,
                            AtomicInteger leafNodeCounter) throws IOException {
    if (node.isLeaf()) {
      leafNodeCounter.incrementAndGet();
    } else {
      branchNodeCounter.incrementAndGet();
    }
    int perLevel = 3;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < (perLevel * level); i = i + perLevel) {
      sb.append("|");
      for (int j = 1; j < perLevel; j++) {
        sb.append("-");
      }
    }
    pw.println(sb.toString() + node);
    if (!node.isLeaf()) {
      for (int i = 0; i <= node.size(); i++) {
        dumpFromNode(tx, pw, tx.readNode(node.getPointer(i)), level + 1, branchNodeCounter, leafNodeCounter);
      }
    }
  }

  @Override
  public boolean verify(Tx<W> tx, Collection<Node.VerifyError> errors) throws IOException {
    tx.begin();
    errors.clear();
    Node rootNode = tx.readNode(tx.getWorkingRootOffset());
    verifyAndTraverse(tx, true, rootNode, errors);
    return errors.isEmpty();
  }

  private void verifyAndTraverse(Tx<W> tx, boolean isRoot, Node node,
                                 Collection<Node.VerifyError> errors) throws IOException {
    node.verifyNodeAndImmediateChildren(errors, isRoot);
    if (!node.isLeaf()) {
      for (int i = 0; i <= node.size(); i++) {
        verifyAndTraverse(tx, false, tx.readNode(node.getPointer(i)), errors);
      }
    }
  }

  public ReadTx<W> readTx() {
    return readTx(DEFAULT_TX_CACHE_SIZE);
  }

  @Override
  public ReadTx<W> readTx(int cacheSize) {
    return new AReadTx<>(this, cacheSize);
  }

  public WriteTx<W> writeTx() {
    return writeTx(DEFAULT_TX_CACHE_SIZE);
  }

  @Override
  public WriteTx<W> writeTx(int cacheSize) {
    return new AWriteTx<>(this, cacheSize);
  }

  /**
   * Gets tree lock.
   *
   * @return the tree lock
   */
  public ReentrantLock getTreeLock() {
    return treeLock;
  }

  /**
   * Gets max bytes per node.
   *
   * @return the max bytes per node
   */
  @Override
  public int getMaxBytesPerNode() {
    return maxBytesPerNode;
  }

  public W createDecorator() {
    return decoratorFactory.create();
  }

  @Override
  public void notifyOfGC(long revision) {
    // can be overridden
  }

  public static int calculateMaxKeyCountForPageSize(int pgSize) {
    int probe = 1;
    while (ANode.maxSizeFor(probe) < pgSize) {
      probe = probe * 2;
    }
    while (ANode.maxSizeFor(probe) > pgSize) {
      probe--;
    }
    return probe;
  }

  @Override
  public IngestHandle startBatch() {
    return startBatch(this.writeTx());
  }

  @Override
  public IngestHandle startBatch(WriteTx<W> wt) {
    return new IngestHandleImpl(wt);
  }
}
