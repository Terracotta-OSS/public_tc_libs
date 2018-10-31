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
package com.terracottatech.sovereign.impl.persistence.hybrid;

import com.terracottatech.sovereign.impl.memory.MemoryLocatorFactory;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.ShardSpec;
import com.terracottatech.sovereign.impl.memory.SovereignRuntime;
import com.terracottatech.sovereign.impl.persistence.base.LSNRetryLookupException;
import com.terracottatech.sovereign.impl.persistence.base.RestartableBufferContainer;
import org.terracotta.offheapstore.paging.PageSource;

import java.nio.ByteBuffer;

/**
 * @author cschanck
 **/
public class HybridBufferContainer extends RestartableBufferContainer {
  private HybridFRSBroker broker;
  private long LSN_BITMASK = (1l << 62);

  public HybridBufferContainer(ShardSpec shardSpec, SovereignRuntime<?> runtime, PageSource pageSource) {
    super(shardSpec, runtime, pageSource);
  }

  @Override
  public ByteBuffer get(PersistentMemoryLocator key) {
    long lastLSN = -1l;
    while (true) {
      try {
        return super.get(key);
      } catch (LSNRetryLookupException e) {
        if (e.getLsn() == lastLSN) {
          throw e.getAssertionError();
        }
        lastLSN = e.getLsn();
      }
    }
  }

  public void setBroker(HybridFRSBroker broker) {
    this.broker = broker;
  }

  boolean isLSNReference(long address) {
    return address > 0 && (address & LSN_BITMASK) != 0;
  }

  // we shift to assure the low bit is not used, which is part of the contract of MemoryAddressList.
  long toLSNReference(long lsn) {
    return (lsn << 1) | LSN_BITMASK;
  }

  // and we unshift here
  long fromLSN(long lsnAddress) {
    if (!isLSNReference(lsnAddress)) {
      throw new IllegalStateException();
    }
    long mask = (LSN_BITMASK - 1l);
    long ret = lsnAddress & mask;
    ret = ret >>> 1;
    return ret;
  }

  @Override
  public PersistentMemoryLocator add(ByteBuffer data) {
    testDisposed();
    // reserve it.
    long index = getLive().reserve();
    PersistentMemoryLocator ret = new PersistentMemoryLocator(index, MemoryLocatorFactory.NONE);
    broker.tapAdd(ret.index(), data);
    return ret;
  }

  public void placeLSN(long slot, long lsn, ByteBuffer rValue) {
    getLive().place(slot, toLSNReference(lsn));
  }

  @Override
  protected ByteBuffer materialize(long address) {
    if (isLSNReference(address)) {
      long lsn = fromLSN(address);
      ByteBuffer probe = broker.getForLSN(lsn);
      return probe;
    }
    return null;
  }

  @Override
  public PersistentMemoryLocator reinstall(long lsn, long persistentKey, ByteBuffer data) {
    getLive().reinstall(persistentKey, toLSNReference(lsn));
    PersistentMemoryLocator loc = new PersistentMemoryLocator(persistentKey, null);
    return loc;
  }

  public boolean delete(PersistentMemoryLocator key) {
    ByteBuffer buffer = get(key);
    if (buffer != null) {
      broker.tapDelete(key.index(), buffer.remaining());
      getLive().clear(key.index());
      return true;
    }
    return false;
  }

  @Override
  protected void free(long uid) {
    if (!isLSNReference(uid)) {
      throw new IllegalStateException();
    }
  }

  @Override
  public PersistentMemoryLocator replace(PersistentMemoryLocator priorKey, ByteBuffer data) {
    ByteBuffer buffer = get(priorKey);
    if (buffer != null) {
      PersistentMemoryLocator ret = new PersistentMemoryLocator(priorKey.index(), MemoryLocatorFactory.NONE);
      broker.tapReplace(priorKey.index(), buffer.remaining(), ret.index(), data);
      return ret;
    } else {
      // just add it
      return add(data);
    }
  }

  @Override
  public PersistentMemoryLocator restore(long key, ByteBuffer data, ByteBuffer oldData) {
    PersistentMemoryLocator loc = super.restore(key, data, oldData);
    if (oldData == null) {
      broker.tapAdd(loc.index(), data);
    } else {
      broker.tapReplace(loc.index(), oldData.remaining(), loc.index(), data);
    }
    return loc;
  }

  public void finishRestart() {
    getLive().finishInitialization();
  }

  @Override
  public void dispose() {
    try {
      if (broker != null) {
        broker.close();
      }
    } finally {
      super.dispose();
    }
  }

  public long getPersistentBytesUsed() {
    return getShardedPersistentBytesUsed(broker);
  }

  public long getOccupiedPersistentSupportStorage() {
    HybridFRSBroker tmp = broker;
    if (tmp != null) {
      return broker.getOccupiedSupportStorage();
    }
    return 0l;
  }

  public long getAllocatedPersistentSupportStorage() {
    HybridFRSBroker tmp = broker;
    if (tmp != null) {
      return broker.getAllocatedSupportStorage();
    }
    return 0l;
  }
}
