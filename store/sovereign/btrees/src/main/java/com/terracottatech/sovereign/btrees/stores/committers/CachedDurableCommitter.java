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

import com.terracottatech.sovereign.common.utils.NIOBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Cached durable committer. Use a durable committer for durable commits, then a local copy for reads/visible
 * commits.
 *
 * @author cschanck
 */
public class CachedDurableCommitter implements DurableCommitter {

  private final DurableCommitter durableCommitter;
  private ByteBuffer transientBuffer;

  /**
   * Instantiates a new Cached durable committer.
   *
   * @param durableCommitter the durable committer
   * @throws IOException the iO exception
   */
  public CachedDurableCommitter(DurableCommitter durableCommitter) throws IOException {
    this.durableCommitter = durableCommitter;
    transientBuffer = ByteBuffer.allocate(durableCommitter.getMaxCommitSize());
    try {
      durableCommitter.getCommitData(transientBuffer);
      transientBuffer.clear();
    } catch (IOException e) {
      transientBuffer = null;
    }
  }

  public void commit(boolean durable, ByteBuffer cBuf) throws IOException {
    if (cBuf.position() != 0) {
      throw new IllegalArgumentException();
    }
    if (durable) {
      this.durableCommitter.commit(cBuf);
    }
    transientBuffer = cBuf;
  }

  /**
   * Gets max commit size.
   *
   * @return the max commit size
   */
  @Override
  public int getMaxCommitSize() {
    return durableCommitter.getMaxCommitSize();
  }

  @Override
  public void commit(ByteBuffer cBuf) throws IOException {
    commit(true, cBuf);
  }

  /**
   * Gets commit data.
   *
   * @param dest the dest
   * @throws IOException the iO exception
   */
  public void getCommitData(ByteBuffer dest) throws IOException {
    if (transientBuffer == null) {
      throw new IOException();
    }
    NIOBufferUtils.copy(transientBuffer, 0, dest, dest.position(),
      Math.min(dest.remaining(), transientBuffer.capacity()));
  }

  public void close() throws IOException {
    durableCommitter.close();
  }

  @Override
  public boolean isDurable() {
    return durableCommitter.isDurable();
  }

  /**
   * Gets total footprint.
   *
   * @return the total footprint
   */
  @Override
  public int getTotalFootprint() {
    return durableCommitter.getTotalFootprint();
  }
}
