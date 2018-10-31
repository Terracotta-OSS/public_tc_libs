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
package com.terracottatech.ehcache.clustered.server.offheap.frs;

import org.terracotta.offheapstore.storage.portability.WriteContext;

import java.nio.ByteBuffer;

/**
 * Data object used to thread chain maps (through a doubly linked list) within the offheap and track live objects.
 *
 * @author RKAV
 */
final class LinkedChainNode {
  private static final int LSN_OFFSET = 0;
  private static final int PREVIOUS_OFFSET = 8;
  private static final int NEXT_OFFSET = 16;
  private static final int LINKED_CHAIN_NODE_LENGTH = 24;

  static final long NULL_ENCODING = Long.MIN_VALUE;

  static final ByteBuffer EMPTY_LINKED_CHAIN_NODE;
  static {
    ByteBuffer emptyHeader = ByteBuffer.allocateDirect(LINKED_CHAIN_NODE_LENGTH);
    emptyHeader.putLong(LSN_OFFSET, -1);
    emptyHeader.putLong(PREVIOUS_OFFSET, NULL_ENCODING);
    emptyHeader.putLong(NEXT_OFFSET, NULL_ENCODING);
    EMPTY_LINKED_CHAIN_NODE = emptyHeader.asReadOnlyBuffer();
  }

  private final ByteBuffer data;
  private final WriteContext writeContext;

  LinkedChainNode(final ByteBuffer buffer, final WriteContext writeContext) {
    this.data = buffer;
    this.writeContext = writeContext;
  }

  long getLsn() {
    return getLong(LSN_OFFSET);
  }

  void setLsn(long lsn) {
    writeContext.setLong(LSN_OFFSET, lsn);
  }

  long getNext() {
    return getLong(NEXT_OFFSET);
  }

  long getPrevious() {
    return getLong(PREVIOUS_OFFSET);
  }

  void setNext(long xtnAddress) {
    writeContext.setLong(NEXT_OFFSET, xtnAddress);
  }

  void setPrevious(long xtnAddress) {
    writeContext.setLong(PREVIOUS_OFFSET, xtnAddress);
  }

  private long getLong(final int address) {
    return data.getLong(address);
  }

  @Override
  public String toString() {
    return getPrevious() + "<== OffHeapLinkedChain [lsn = " + getLsn() + "] ==> " + getNext();
  }
}