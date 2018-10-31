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
package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.VersionLimitStrategy;
import com.terracottatech.sovereign.impl.SovereignAllocationResource;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaImpl;
import com.terracottatech.sovereign.impl.memory.recordstrategies.codec.VersionedRecordCodec;
import com.terracottatech.sovereign.impl.memory.recordstrategies.simple.SimpleRecordBufferStrategy;
import com.terracottatech.sovereign.impl.memory.recordstrategies.valuepilecodec.ValuePileRecordBufferStrategy;
import com.terracottatech.sovereign.spi.SpaceRuntime;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.sovereign.impl.utils.LockSet;
import com.terracottatech.sovereign.common.utils.SimpleFinalizer;
import com.terracottatech.sovereign.impl.utils.TriPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * @author cschanck
 **/
public class SovereignRuntime<K extends Comparable<K>> implements SpaceRuntime {
  private static Logger LOG = LoggerFactory.getLogger(SovereignRuntime.class);
  private SovereignDataSetConfig<K, ? extends TimeReference<?>> config;
  private final LockSet lockset;
  private final TriPredicate<TimeReference<?>, TimeReference<?>, Integer> recordConstructionFilter;
  private CachingSequence sequence;
  private final BiFunction<TimeReference<?>, TimeReference<?>, VersionLimitStrategy.Retention> recordRetrievalFilter;
  private RecordBufferStrategy<K> bufferStrategy;
  private final KeySlotShardEngine shardEngine;
  private volatile boolean closed = false;
  private SimpleFinalizer<ContextImpl, ContextImpl.State> contextFinalizer = new SimpleFinalizer<ContextImpl, ContextImpl.State>(
    ContextImpl.State.class,
    30,
    TimeUnit.SECONDS,
    (c) -> {
      if (closed) {
        return;
      }
      if (c.isOpen()) {
        LOG.warn(
          "Context allocated by {} was not explicitly closed; failure to close Stream instances obtained from a SovereignDataset may result in failures and/or unexpected behavior caused by resource exhaustion.",
          c.getClass().getSimpleName(),
          c.getAllocationStackTrace());
        try {
          c.close();
        } catch (Throwable t) {
        }
      }
    });
  private final DatasetSchemaImpl schema = new DatasetSchemaImpl();

  public SovereignRuntime(SovereignDataSetConfig<K, ?> config, CachingSequence sequence) {
    this.config = config;
    this.sequence = sequence;
    this.recordConstructionFilter = config.makeRecordConstructionFilter();
    this.recordRetrievalFilter = getConfig().makeRecordRetrievalFilter();
    switch(config.getBufferStrategyType()) {
      case Versioned:
        this.bufferStrategy= new VersionedRecordCodec<>(config);
        break;
      case Simple:
        this.bufferStrategy = new SimpleRecordBufferStrategy<>(this.getConfig(), schema);
        break;
      case VP:
        this.bufferStrategy = new ValuePileRecordBufferStrategy<K>(this.getConfig(), schema, false);
        break;
      case VPLazy:
        this.bufferStrategy = new ValuePileRecordBufferStrategy<K>(this.getConfig(), schema, true);
        break;
      default:
        throw new IllegalArgumentException(config.getBufferStrategyType().toString());
    }

    // we expect concurrency to be power of two.
    this.shardEngine = new KeySlotShardEngine(config.getConcurrency());
    this.lockset = new LockSet(shardEngine, config.isFairLocking());
  }

  public SovereignAllocationResource allocator() {
    return config.getStorage().getAllocator();
  }

  public CachingSequence getSequence() {
    return sequence;
  }

  public void setSequence(CachingSequence sequence) {
    this.sequence = sequence;
  }

  public TimeReferenceGenerator<? extends TimeReference<?>> getTimeReferenceGenerator() {
    return config.getTimeReferenceGenerator();
  }

  public SovereignDataSetConfig<K, ? extends TimeReference<?>> getConfig() {
    return config;
  }

  public KeySlotShardEngine getShardEngine() {
    return shardEngine;
  }

  public BiFunction<TimeReference<?>, TimeReference<?>, VersionLimitStrategy.Retention> getRecordRetrievalFilter() {
    return recordRetrievalFilter;
  }

  public LockSet getLockset() {
    return lockset;
  }

  public TriPredicate<TimeReference<?>, TimeReference<?>, Integer> getRecordConstructionFilter() {
    return recordConstructionFilter;
  }

  public int getMaxResourceChunkSize() {
    return config.maxResourceChunkSize();
  }

  public long getResourceSize() {
    return config.getResourceSize();
  }

  public RecordBufferStrategy<K> getBufferStrategy() {
    return bufferStrategy;
  }

  public SimpleFinalizer<ContextImpl, ContextImpl.State> getContextFinalizer() {
    return contextFinalizer;
  }

  public void dispose() {
    if (!closed) {
      closed = true;
      contextFinalizer.close();
      config = null;            // Drop indirect reference to off-heap storage
      bufferStrategy = null;    // Drop indirect reference to off-heap storage
    }
  }

  public DatasetSchemaImpl getSchema() {
    return schema;
  }
}
