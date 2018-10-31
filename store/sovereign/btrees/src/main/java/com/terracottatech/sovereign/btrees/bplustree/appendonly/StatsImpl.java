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

import com.terracottatech.sovereign.btrees.bplustree.model.Stats;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The volatiles can be increment because they are recording mutative operations only,
 * which will happen under lock.
 *
 * @author cschanck
 **/

@SuppressFBWarnings({"VO_VOLATILE_INCREMENT"})
public class StatsImpl implements Stats {
  private volatile long inserts;
  private volatile long deletes;
  private AtomicLong gets = new AtomicLong();
  private volatile long branchNodeSplits;
  private volatile long leafNodeSplits;
  private volatile long branchNodeMerges;
  private volatile long leafLeftToRight;
  private volatile long branchLeftToRight;
  private volatile long leafNodeMerges;
  private volatile long leafRightToLeft;
  private AtomicLong discards = new AtomicLong();
  private AtomicLong commits = new AtomicLong();
  private volatile long branchRightToLeft;

  @Override
  public long inserts() {
    return inserts;
  }

  @Override
  public long deletes() {
    return deletes;
  }

  @Override
  public long gets() {
    return gets.get();
  }

  @Override
  public long branchNodeSplits() {
    return branchNodeSplits;
  }

  @Override
  public long leafNodeSplits() {
    return leafNodeSplits;
  }

  @Override
  public long branchNodeMerges() {
    return branchNodeMerges;
  }

  @Override
  public long leafNodeMerges() {
    return leafNodeMerges;
  }

  @Override
  public long branchLeftToRight() {
    return branchLeftToRight;
  }

  @Override
  public long leafLeftToRight() {
    return leafLeftToRight;
  }

  @Override
  public long branchRightToLeft() {
    return branchLeftToRight;
  }

  @Override
  public long leafRightToLeft() {
    return leafRightToLeft;
  }

  @Override
  public long commits() {
    return commits.get();
  }

  @Override
  public long discards() {
    return discards.get();
  }

  public void recordInsert() {
    inserts++;
  }

  public void recordDelete() {
    deletes++;
  }

  public void recordGet() {
    gets.incrementAndGet();
  }

  public void recordBranchNodeSplit() {
    branchNodeSplits++;
  }

  public void recordLeafNodeSplit() {
    leafNodeSplits++;
  }

  public void recordBranchNodeMerges() {
    branchNodeMerges++;
  }

  public void recordLeafLeftToRight() {
    leafLeftToRight++;
  }

  public void recordBranchleftToRight() {
    branchLeftToRight++;
  }

  public void recordLeafNodeMerges() {
    leafNodeMerges++;
  }

  public void recordLeafRightToLeft() {
    leafRightToLeft++;
  }

  public void recordDiscard() {
    discards.incrementAndGet();
  }

  public void recordCommit() {
    commits.incrementAndGet();
  }

  @Override
  public String toString() {
    return "StatsImpl{" +
      "gets=" + gets +
      ", inserts=" + inserts +
      ", deletes=" + deletes +
      ", discards=" + discards +
      ", commits=" + commits +
      ", branchNodeSplits=" + branchNodeSplits +
      ", leafNodeSplits=" + leafNodeSplits +
      ", leafNodeMerges=" + leafNodeMerges +
      ", branchNodeMerges=" + branchNodeMerges +
      ", leafLeftToRight=" + leafLeftToRight +
      ", branchLeftToRight=" + branchLeftToRight +
      ", leafRightToLeft=" + leafRightToLeft +
      ", branchRightToLeft=" + branchRightToLeft +
      '}';
  }

  public void recordBranchRightToLeft() {
    branchRightToLeft++;
  }
}
