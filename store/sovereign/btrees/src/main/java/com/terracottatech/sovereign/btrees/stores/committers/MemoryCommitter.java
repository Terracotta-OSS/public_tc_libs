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
 * In Memory committer. Obvs not durable.
 *
 * @author cschanck
 */
public class MemoryCommitter implements Committer {

  private volatile ByteBuffer cbuf = null;

  /**
   * Gets max commit size.
   *
   * @return the max commit size
   */
  @Override
  public int getMaxCommitSize() {
    return 64 * 1024;
  }

  @Override
  public void commit(ByteBuffer cBuf) {
    cbuf = cBuf;
  }

  /**
   * Gets commit data.
   *
   * @param dest the dest
   * @throws IOException the iO exception
   */
  @Override
  public void getCommitData(ByteBuffer dest) throws IOException {
    ByteBuffer tmp = cbuf;
    if (tmp == null) {
      throw new IOException();
    }
    NIOBufferUtils.copy(tmp, 0, dest, dest.position(), Math.min(dest.remaining(), tmp.capacity()));
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean isDurable() {
    return false;
  }
}
