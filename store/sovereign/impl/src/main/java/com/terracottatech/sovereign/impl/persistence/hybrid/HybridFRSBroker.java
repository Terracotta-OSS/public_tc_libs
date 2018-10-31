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

import com.terracottatech.frs.RestartStore;
import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import com.terracottatech.sovereign.common.utils.SimplestLRUCache;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.memory.ShardedRecordContainer;
import com.terracottatech.sovereign.impl.persistence.base.SovereignRestartableBroker;

import java.nio.ByteBuffer;

/**
 * @author cschanck
 **/
public class HybridFRSBroker extends SovereignRestartableBroker<HybridRecordContainer<?>> {
  private final ShardedRecordContainer<?, ?> hybridContainer;
  private final SimplestLRUCache.Segmented<Long, ByteBuffer> cache;

  public HybridFRSBroker(ByteBuffer identifier,
                         RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartability,
                         SovereignDatasetImpl<?> dataset) {
    super(identifier, restartability, dataset);
    this.hybridContainer = dataset.getContainer();
    // create a very tiny sharded cache, used to avoid disk lookups if possible.
    // critically important because we double fetch a lot in practice,
    // and write+fetch a lot in tests.
    this.cache = new SimplestLRUCache.Segmented<>(dataset.getRuntime().getShardEngine().getShardCount(),
                                                  dataset.getRuntime().getShardEngine().getShardCount() * 32);
  }

  @Override
  public ByteBuffer getForLSN(long lsn) {
    ByteBuffer probe = cache.get(lsn);
    if (probe != null) {
      return probe;
    }
    ByteBuffer ret = super.getForLSN(lsn);
    if (ret != null) {
      cache.put(lsn, NIOBufferUtils.dup(ret,true));
    }
    return ret;
  }

  @Override
  protected void notifyOfLSNAssignment(Long key, long lsn, ByteBuffer rValue) {
    HybridBufferContainer shard = ((HybridRecordContainer) hybridContainer.shardForSlot(key.longValue())).getBufferContainer();
    shard.placeLSN(key.longValue(), lsn, rValue);
    cache.put(lsn, NIOBufferUtils.dup(rValue, true));
  }

  @Override
  protected void notifyOfLSNUpdate(Long key, long priorLSN, long newLSN) {
    HybridBufferContainer shard = ((HybridRecordContainer) hybridContainer.shardForSlot(key.longValue())).getBufferContainer();
    shard.placeLSN(key.longValue(), newLSN, null);
  }

}
