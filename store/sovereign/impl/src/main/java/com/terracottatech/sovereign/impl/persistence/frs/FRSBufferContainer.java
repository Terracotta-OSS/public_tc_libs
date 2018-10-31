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
package com.terracottatech.sovereign.impl.persistence.frs;

import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.ShardSpec;
import com.terracottatech.sovereign.impl.memory.SovereignRuntime;
import com.terracottatech.sovereign.impl.persistence.base.RestartableBufferContainer;
import org.terracotta.offheapstore.paging.PageSource;

import java.nio.ByteBuffer;

/**
 * @author cschanck
 **/
public class FRSBufferContainer extends RestartableBufferContainer {
  private FRSBroker broker;

  FRSBufferContainer(ShardSpec shardSpec, SovereignRuntime<?> runtime, PageSource pageSource) {
    super(shardSpec, runtime, pageSource);
  }

  public void setBroker(FRSBroker broker) {
    this.broker = broker;
  }

  public long getPersistentBytesUsed() {
    return getShardedPersistentBytesUsed(broker);
  }

  @Override
  public PersistentMemoryLocator add(ByteBuffer data) {
    PersistentMemoryLocator ret = super.add(data);
    broker.tapAdd(ret.index(), data);
    return ret;
  }

  @Override
  public boolean delete(PersistentMemoryLocator key) {
    ByteBuffer buf = super.get(key);
    boolean ret = super.delete(key);
    broker.tapDelete(key.index(), buf.remaining());
    return ret;
  }

  @Override
  public PersistentMemoryLocator replace(PersistentMemoryLocator priorKey, ByteBuffer data) {
    // ordering here is important -- fetching the old data must happen before
    // the super.replace() call.
    int oldDataSize=0;
    if (priorKey != null) {
      ByteBuffer b = super.get(priorKey);
      if (b != null) {
        oldDataSize = b.remaining();
      } else {
        oldDataSize = 0;
      }
    }
    PersistentMemoryLocator ret = super.replace(priorKey, data);
    if (priorKey == null) {
      broker.tapAdd(ret.index(), data);
    } else {
      if (ret.index() != priorKey.index()) {
        broker.tapReplace(priorKey.index(), oldDataSize, ret.index(), data);
        getLive().clear(priorKey.index());
      } else {
        broker.tapReplace(priorKey.index(), oldDataSize, ret.index(), data);
      }
    }
    return ret;
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

  @Override
  public void dispose() {
    try {
      if(broker!=null) {
        broker.close();
      }
    } finally {
      super.dispose();
    }
  }

  public void finishRestart() {
    getLive().finishInitialization();
  }

  public long getAllocatedPersistentSupportStorage() {
    FRSBroker tmp = broker;
    if (tmp != null) {
      return tmp.getAllocatedSupportStorage();
    }
    return 0;
  }

  public long getOccupiedPersistentSupportStorage() {
    FRSBroker tmp = broker;
    if (tmp != null) {
      return tmp.getOccupiedSupportStorage();
    }
    return 0;
  }
}
