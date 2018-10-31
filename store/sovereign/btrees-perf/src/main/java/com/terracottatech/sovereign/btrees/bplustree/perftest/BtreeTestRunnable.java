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
package com.terracottatech.sovereign.btrees.bplustree.perftest;

import com.terracottatech.sovereign.btrees.bplustree.appendonly.ABPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.CommitType;
import com.terracottatech.sovereign.btrees.bplustree.model.Node;
import com.terracottatech.sovereign.btrees.bplustree.model.Tx;
import com.terracottatech.sovereign.btrees.bplustree.model.WriteTx;
import com.terracottatech.sovereign.common.utils.SW;
import com.terracottatech.sovereign.common.utils.SequenceGenerator;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

/**
 * The type Btree test runnable. Used per thread to warmup and test.
 *
 * @author cschanck
 */
class BtreeTestRunnable implements Runnable {
  private final int myIndex;
  private final int totalValueCount;
  private final CyclicBarrier barrier;
  private final ABPlusTree<?> tree;
  private final boolean testWrites;
  private final boolean orderedInserts;
  private final int hotSetSize;

  private volatile int tps;
  private volatile int warmUpTps;

  /**
   * Instantiates a new Btree test runnable.
   *
   * @param index          the index
   * @param valueNum       the value num
   * @param hotSetSize     the hot set size
   * @param testWrites     the test writes
   * @param tree           the tree
   * @param barrier        the barrier
   * @param orderedInserts the ordered inserts
   */
  public BtreeTestRunnable(final int index, final int valueNum, final int hotSetSize,
                           final boolean testWrites, final ABPlusTree<?> tree, final CyclicBarrier barrier,
                           boolean orderedInserts) {

    this.myIndex = index;
    this.orderedInserts = orderedInserts;
    this.hotSetSize = hotSetSize;
    this.totalValueCount = valueNum;
    this.testWrites = testWrites;
    this.tree = tree;
    this.barrier = barrier;
  }

  @Override
  public void run() {
    try {
      warmUp();
      try {
        barrier.await();
      } catch (InterruptedException e) {
        return;
      } catch (BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
      runTest();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void warmUp() throws IOException {
    if (myIndex != 0) {
      warmUpTps = 0;
      return;
    }
    SequenceGenerator seq = new SequenceGenerator(totalValueCount, false);
    final long startTime = System.currentTimeMillis();
    WriteTx<?> tx = tree.writeTx(2000);
    int cnt = 0;
    SW sw = new SW();
    for (int i = 0; i < totalValueCount; i++) {
      int ii = i;
      if (!orderedInserts) {
        ii = seq.next();
      }
      tx.insert(ii, ii);
      cnt++;
      if ((cnt % 1000) == 0) {
        tx.commit(CommitType.VISIBLE);
      }
      if ((cnt % 100000) == 0) {
        tx.commit(CommitType.DURABLE);
      }
      if ((cnt % 1000000) == 0) {
        System.out.println("Thread #" + myIndex + " " + sw.mark(1000000));
      }
    }
    tx.commit(CommitType.DURABLE);

    tx.close();
    long time = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
    if (time == 0) {
      time = 1;
    }
    warmUpTps = (int) (totalValueCount / time);
    System.out.println("Thread #" + myIndex + " done warming up: " + totalValueCount +
      " keys inserted in " + time + " seconds: " + warmUpTps + " tps " + "(" + (orderedInserts ? "ordered)" : "random)"));
  }

  protected void runTest() {
    final long startTime = System.currentTimeMillis();
    long transactions = 0;
    SequenceGenerator hotSet = new SequenceGenerator(hotSetSize, true);
    try {
      while (!Thread.interrupted()) {
        if (myIndex == 0 && testWrites) {
          WriteTx<?> tx = tree.writeTx();
          long l = hotSet.next();
          tx.insert(l, l);
          tx.commit(CommitType.VISIBLE);
          tx.close();
        } else {
          Tx<?> tx = tree.readTx();
          long k = hotSet.next();
          if (tx.find(k) == null) {
            System.out.println("MISS! " + k);
            LinkedList<Node.VerifyError> errs = new LinkedList<Node.VerifyError>();
            if (!tx.verify(errs)) {
              System.out.println("Fail@" + k);
              for (Node.VerifyError ve : errs) {
                System.out.println(ve);
              }
              System.out.println();
            }
          }
          tx.close();
        }
        transactions++;
      }
    } catch (Exception e) {
    }
    long time = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
    tps = (int) (transactions / time);
    System.out.println(
      "Thread #" + myIndex + " done testing: " + transactions + " keys accessed in " + time + " seconds: " + tps + " tps");
  }

  /**
   * Gets tps.
   *
   * @return the tps
   */
  public int getTps() {
    return tps;
  }

  /**
   * Gets warm up tps.
   *
   * @return the warm up tps
   */
  public int getWarmUpTps() {
    return warmUpTps;
  }
}
