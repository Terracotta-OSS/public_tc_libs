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

import java.nio.ByteBuffer;

/**
 * Snapshot, captures a mvcc view of the tree.
 *
 * @author cschanck
 */
public class SnapShot {

  private final long revision;

  private final long rootAddress;

  private final ByteBuffer commitData;

  private final long size;
  private final int height;

  /**
   * Instantiates a new Snap shot.
   *
   * @param rootAddress the root address
   * @param revision the revision
   * @param commitData the commit data
   * @param size the size
   * @param height the height
   */
  public SnapShot(long rootAddress, long revision, ByteBuffer commitData, long size, int height) {
    this.revision = revision;
    this.size = size;
    this.rootAddress = rootAddress;
    this.commitData = commitData;
    this.height = height;
  }

  /**
   * Gets revision.
   *
   * @return the revision
   */
  public long getRevision() {
    return revision;
  }

  /**
   * Gets root address.
   *
   * @return the root address
   */
  public long getRootAddress() {
    return rootAddress;
  }

  /**
   * Gets commit data.
   *
   * @return the commit data
   */
  public ByteBuffer getCommitData() {
    return commitData;
  }

  /**
   * Gets size.
   *
   * @return the size
   */
  public long getSize() {
    return size;
  }

  /**
   * Gets height.
   *
   * @return the height
   */
  public int getHeight() {
    return height;
  }
}
