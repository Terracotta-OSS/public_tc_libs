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
package com.terracottatech.sovereign.btrees.duplicate;

import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import com.terracottatech.sovereign.common.dumbstruct.Accessor;
import com.terracottatech.sovereign.common.dumbstruct.buffers.DataBuffer;
import com.terracottatech.sovereign.common.dumbstruct.buffers.SingleDataByteBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * List of longs. TODO: keep them sorted. Faster.
 *
 * @author cschanck
 */
public class LongList {
  private final int cap;
  private ByteBuffer buffer;
  private Accessor accessor;
  private final int pgSize;
  private boolean dirty;
  private int size;
  private long revision;
  private DataBuffer db;

  public LongList(int pgSize) {
    this.buffer = ByteBuffer.allocate(pgSize);
    this.cap = LongListProxy.listSizeForPageSize(buffer.capacity());
    dirty = false;
    this.pgSize = pgSize;
    wrap(buffer);
    size = 0;
    revision = -1L;
  }

  public LongList(ByteBuffer b, int pgSize) {
    this.buffer = b;
    this.cap = LongListProxy.listSizeForPageSize(b.capacity());
    dirty = false;
    this.pgSize = pgSize;
    wrap(b);
    size = LongListProxy.PROXY.size(accessor);
    revision = LongListProxy.PROXY.getRevision(accessor);
  }

  private void wrap(ByteBuffer b) {
    this.db = new SingleDataByteBuffer(b);
    this.accessor = new Accessor(db);
  }

  public int size() {
    return size;
  }

  public long get(int index) {
    return LongListProxy.PROXY.get(accessor, index);
  }

  public void add(long v) {
    set(size, v);
  }

  public long getRevision() {
    return revision;
  }

  private void insert(int index, long val) {
    ensureCapacity(size + 1);
    LongListProxy.PROXY.insert(size, accessor, index, val);
    size++;
    dirty = true;
  }

  private void delete(int index) {
    if (index < size) {
      LongListProxy.PROXY.delete(size, accessor, index);
      dirty = true;
      size--;
    }
  }

  private void set(int index, long val) {
    ensureCapacity(index + 1);
    LongListProxy.PROXY.set(accessor, index, val);
    size = Math.max(size, index + 1);
    dirty = true;
  }

  public void setRevision(long rev) {
    revision = rev;
    dirty = true;
  }

  private void ensureCapacity(int i) {
    if (i > cap) {
      ByteBuffer tmpBuffer = ByteBuffer.allocate(db.size() + pgSize);
      NIOBufferUtils.copy(buffer, tmpBuffer);
      buffer = tmpBuffer;
      wrap(buffer);
    }
  }

  public ByteBuffer marshalForWrite() {
    LongListProxy.PROXY.setSize(accessor, (short) size);
    LongListProxy.PROXY.setRevision(accessor, revision);
    ByteBuffer ret = buffer.duplicate();
    ret.position(0);
    ret.limit(LongListProxy.byteSizeForListCount(size));
    return ret.slice().asReadOnlyBuffer();
  }

  public boolean isDirty() {
    return dirty;
  }

  public List<Long> asList() {
    ArrayList<Long> list = new ArrayList<Long>(size);
    for (int i = 0; i < size; i++) {
      list.add(get(i));
    }
    return list;
  }

  public boolean remove(long val) {
    for (int i = 0; i < size(); i++) {
      if (get(i) == val) {
        delete(i);
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return asList().toString();
  }
}
