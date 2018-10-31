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
package com.terracottatech.ehcache.common.frs;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.object.ObjectManagerSegment;
import com.terracottatech.frs.object.ObjectManagerStripe;
import com.terracottatech.frs.object.RestartableObject;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

/**
 * This restartable object is a dummy restartable object that supports only delete transactions.
 * <p>
 * Elements of the given object identified by {@code identifier} recovered using this restartable object cannot be used for
 * any purpose other than to issue a delete on the identifier. It is assumed that the {@link RestartStore} containing this
 * object is closed after issuing deletes on this object.
 *
 * @author RKAV
 */
public class DeleteOnlyRestartableObject implements RestartableObject<ByteBuffer, ByteBuffer, ByteBuffer> {
  private final ByteBuffer id;
  private final RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore;
  private final ObjectManagerStripe<ByteBuffer, ByteBuffer, ByteBuffer> objectManagerStripe;

  public DeleteOnlyRestartableObject(final ByteBuffer id, final RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore) {
    this.id = id;
    this.restartStore = restartStore;
    this.objectManagerStripe = new DummyObjectManagerStripe();
  }

  @Override
  public ByteBuffer getId() {
    return id;
  }

  @Override
  public ObjectManagerStripe<ByteBuffer, ByteBuffer, ByteBuffer> getObjectManagerStripe() {
    return this.objectManagerStripe;
  }

  public void destroy() {
    try {
      restartStore.beginAutoCommitTransaction(true).delete(id).commit();
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }

  private static final class DummyObjectManagerStripe implements ObjectManagerStripe<ByteBuffer, ByteBuffer, ByteBuffer>,
      ObjectManagerSegment<ByteBuffer, ByteBuffer, ByteBuffer> {
    @Override
    public Long getLowestLsn() {
      return 0L;
    }

    @Override
    public Long getLsn(int i, ByteBuffer byteBuffer) {
      return 0L;
    }

    @Override
    public void put(int i, ByteBuffer byteBuffer, ByteBuffer byteBuffer2, long l) {
    }

    @Override
    public void replayPut(int i, ByteBuffer byteBuffer, ByteBuffer byteBuffer2, long l) {
    }

    @Override
    public void remove(int i, ByteBuffer byteBuffer) {
    }

    @Override
    public Long getLsn(ByteBuffer byteBuffer) {
      return 0L;
    }

    @Override
    public void put(ByteBuffer byteBuffer, ByteBuffer byteBuffer2, long l) {
    }

    @Override
    public void remove(ByteBuffer byteBuffer) {
    }

    @Override
    public void delete() {
    }

    @Override
    public void replayPut(ByteBuffer byteBuffer, ByteBuffer byteBuffer2, long l) {

    }

    @Override
    public Collection<ObjectManagerSegment<ByteBuffer, ByteBuffer, ByteBuffer>> getSegments() {
      return Collections.<ObjectManagerSegment<ByteBuffer, ByteBuffer, ByteBuffer>>singleton(this);
    }

    @Override
    public void updateLsn(ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> objectManagerEntry, long l) {

    }

    @Override
    public ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> acquireCompactionEntry(long l) {
      return null;
    }

    @Override
    public void updateLsn(int i, ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> objectManagerEntry, long l) {

    }

    @Override
    public void releaseCompactionEntry(ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> objectManagerEntry) {

    }

    @Override
    public long size() {
      return 0;
    }

    @Override
    public long sizeInBytes() {
      return 0;
    }
  }
}