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
package com.terracottatech.sovereign.btrees.stores.committers;

import com.terracottatech.sovereign.common.utils.FileUtils;
import com.terracottatech.sovereign.common.utils.NIOBufferUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.terracottatech.sovereign.common.utils.FileUtils.fileChannel;

/**
 * Durable file committer. Uses normal file channel IO to commit.
 *
 * @author cschanck
 */
public class DurableFileCommitter implements DurableCommitter {
  private static final int OVERHEAD = 12;

  private final File file;
  private final int maxCommitSize;
  private final FileChannel provider;
  private long commitSeq = 0;
  private int currentBlock = -1;
  private long[] locations = new long[2];

  /**
   * Instantiates a new Durable file committer.
   *
   * @param f the f
   * @param maxCommitSize the max commit size
   * @throws IOException the iO exception
   */
  public DurableFileCommitter(File f, int maxCommitSize) throws IOException {
    this(f, 0L, maxCommitSize);
  }

  /**
   * Instantiates a new Durable file committer.
   *
   * @param f the f
   * @param position the position
   * @param maxCommitSize the max commit size
   * @throws IOException the iO exception
   */
  public DurableFileCommitter(File f, long position, int maxCommitSize) throws IOException {
    this.file = f;
    this.provider = fileChannel(f);
    this.maxCommitSize = maxCommitSize;
    locations[0] = position;
    locations[1] = position + maxCommitSize + OVERHEAD;
    init();
  }

  private boolean init() {
    long firstSeq = readSequence(0);
    long secondSeq = readSequence(1);
    if (firstSeq > 0) {
      if (secondSeq > firstSeq) {
        currentBlock = 1;
      } else {
        currentBlock = 0;
      }
    } else if (secondSeq > 0) {
      currentBlock = 1;
    } else {
      return false;
    }
    return true;
  }

  /**
   * Gets max commit size.
   *
   * @return the max commit size
   */
  @Override
  public int getMaxCommitSize() {
    return maxCommitSize;
  }

  @Override
  public void commit(ByteBuffer cBuf) throws IOException {
    int next = (currentBlock < 0) ? 0 : 1 - currentBlock;
    ByteBuffer b = ByteBuffer.allocate(maxCommitSize + OVERHEAD);
    int many = cBuf.remaining();
    b.putLong(++commitSeq);
    b.putInt(many);
    b.put(cBuf.slice());
    b.clear();
    b.limit(many + OVERHEAD);
    FileUtils.writeFully(provider, b, locations[next]);
    b.clear();
    provider.force(false);
    currentBlock = next;
  }

  /**
   * Gets commit data.
   *
   * @param dest the dest
   * @throws IOException the iO exception
   */
  @Override
  public void getCommitData(ByteBuffer dest) throws IOException {
    if (currentBlock < 0) {
      throw new IOException();
    }
    ByteBuffer b = ByteBuffer.allocate(maxCommitSize + OVERHEAD);
    FileUtils.readFully(provider, b, locations[currentBlock]);
    int len = b.getInt(8);
    NIOBufferUtils.copy(b, OVERHEAD, dest, dest.position(), len);
    dest.limit(dest.position() + len);
  }

  @Override
  public void close() throws IOException {
    provider.close();
  }

  @Override
  public boolean isDurable() {
    return true;
  }

  private long readSequence(int blockNumber) {
    ByteBuffer buf = ByteBuffer.allocate(8);
    try {
      FileUtils.readFully(provider, buf, locations[blockNumber]);
      buf.clear();
      return buf.getLong(0);
    } catch (IOException e) {
      return -1L;
    }
  }

  public String toString() {
    return "Seq 0: " + readSequence(0) + " Seq 1: " + readSequence(1);
  }

  /**
   * Gets total footprint.
   *
   * @return the total footprint
   */
  @Override
  public int getTotalFootprint() {
    return 2 * (maxCommitSize + OVERHEAD);
  }
}
