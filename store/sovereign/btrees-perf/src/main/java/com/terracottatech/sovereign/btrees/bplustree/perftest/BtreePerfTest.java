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
import com.terracottatech.sovereign.btrees.bplustree.appendonly.DUT;
import com.terracottatech.sovereign.common.utils.TimedWatcher;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

/**
 * The type Btree perf test. This is a class used to test raw ABplusTree's. See the set of
 * properties int he test() method for a rundown on what it can do.
 *
 * @author cschanck
 */
public final class BtreePerfTest {

  private BtreePerfTest() {
  }

  private static Runnable makeWatcher() {
    return new Runnable() {
      private long start = System.currentTimeMillis();

      @Override
      public void run() {
        long elapsed = System.currentTimeMillis() - start;
        float minutes = (float) elapsed / (1000.0f * 60.0f);
        System.out.println(String.format("%3.1f minutes", minutes));
      }
    };
  }

  public static void test(final ABPlusTree<?> tree) throws InterruptedException, BrokenBarrierException, IOException {
    final int threadsNum = Integer.getInteger("threads", 16);
    final int valueNum = Integer.getInteger("values.num", 100 * 1000 * 1000);
    final int userHotSet = Integer.getInteger("test.hotset", -1);
    final int testLengthInMinutes = Integer.getInteger("test.length", 5);
    final boolean testWrites = Boolean.parseBoolean(System.getProperty("test.writes", "True"));
    final boolean orderedInserts = Boolean.parseBoolean(System.getProperty("test.orderedInserts", "false"));
    final int hotSetSize = userHotSet > 0 ? Math.min(userHotSet, valueNum) : valueNum;

    Set<BtreeTestRunnable> cacheTestRunnables = new HashSet<>();
    System.out.println("*******************************");
    System.out.println("*  WELCOME TO THIS PERF TEST  *");
    System.out.println("*******************************");

    CyclicBarrier barrier = new CyclicBarrier(threadsNum + 1);
    final Thread[] threads = new Thread[threadsNum];
    for (int i = 0; i < threads.length; i++) {
      final BtreeTestRunnable cacheTestRunnable = new BtreeTestRunnable(i, valueNum, hotSetSize, testWrites,
        tree, barrier, orderedInserts);
      cacheTestRunnables.add(cacheTestRunnable);
      threads[i] = new Thread(cacheTestRunnable);
      threads[i].setDaemon(true);
      threads[i].start();
    }
    barrier.await();

    System.out.println("Tree Store Stats: " + tree.getTreeStore().getStats().toString());
    int warmUpTps = 0;
    for (BtreeTestRunnable cacheTestRunnable : cacheTestRunnables) {
      warmUpTps += cacheTestRunnable.getWarmUpTps();
    }

    System.out.println("WARM UP TPS: " + warmUpTps);
    System.out.println(
      "Running actual test for ~" + testLengthInMinutes + " minute" + (testLengthInMinutes > 1 ? "s" : ""));

    TimerTask task = TimedWatcher.watch(60 * 1000, makeWatcher());

    Thread.sleep(TimeUnit.MINUTES.toMillis(testLengthInMinutes));

    task.cancel();
    System.out.println("Done... everyone stops!");
    for (Thread thread : threads) {
      thread.interrupt();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    int totalTps = 0;
    for (BtreeTestRunnable cacheTestRunnable : cacheTestRunnables) {
      totalTps += cacheTestRunnable.getTps();
    }

//    try {
//      tree.close();
//    } catch (IOException e) {
//    }

    System.out.println("***************************************************************");
    System.out.println("*  WE'VE REACHED OUR DESTINATION, HOPE TO SEE YOU AGAIN SOON  *");
    System.out.println("*                                      (tm) Chris Schanck     *");
    System.out.println("*                                      (homage:  Alex Snaps)  *");
    System.out.println("***************************************************************");
    System.out.println("Tree Store Stats: " + tree.getTreeStore().getStats().toString());
    System.out.println("Tested with " + threads.length + " threads" +
      " on " + valueNum + " values" +
      " for " + testLengthInMinutes + " minutes" +
      " with a read ratio of " + readRatio(testWrites, threadsNum));
    System.out.println((orderedInserts ? "Ordered" : "PRandom") + " inserts used for warmup");
    System.out.println("TOTAL TPS: " + totalTps);
  }

  private static String readRatio(boolean writes, int num) {
    if (!writes) {
      return "100";
    }

    return (((num - 1) * 100) / num) + "";
  }

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws InterruptedException   the interrupted exception
   * @throws BrokenBarrierException the broken barrier exception
   * @throws IOException            the iO exception
   */
  public static void main(String[] args) throws InterruptedException, BrokenBarrierException, IOException {
    //BtreePerfTest.test(DUT.diskUnlimitedUnmapped(32));
    BtreePerfTest.test(DUT.offheap(32));
  }

}
