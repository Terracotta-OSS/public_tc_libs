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
package com.terracottatech.sovereign.impl.persistence.base;

import com.terracottatech.sovereign.impl.memory.MemoryBufferContainer;
import com.terracottatech.sovereign.impl.memory.ShardSpec;
import com.terracottatech.sovereign.impl.memory.SovereignRuntime;
import com.terracottatech.sovereign.impl.model.PersistableDataContainer;
import org.terracotta.offheapstore.paging.PageSource;

public abstract class RestartableBufferContainer extends MemoryBufferContainer {
  private final ShardSpec shardSpec;

  protected RestartableBufferContainer(ShardSpec shardSpec, SovereignRuntime<?> runtime, PageSource pageSource) {
    super(shardSpec, runtime, pageSource);
    this.shardSpec = shardSpec;
  }

  protected <W extends PersistableDataContainer<?, ?>> long getShardedPersistentBytesUsed(SovereignRestartableBroker<W> broker) {
    if (broker == null) {
      return 0;
    }

    long unshardedByteSize = broker.getByteSize();
    int shardCount = shardSpec.getShardCount();

    return unshardedByteSize / shardCount;
  }
}
