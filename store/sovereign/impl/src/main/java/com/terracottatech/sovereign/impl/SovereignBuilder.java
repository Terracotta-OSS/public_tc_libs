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
package com.terracottatech.sovereign.impl;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.VersionLimitStrategy;
import com.terracottatech.sovereign.impl.persistence.AbstractStorage;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.store.Type;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Supports configuring and instantiating a new {@link SovereignDataset} instance.
 * <p>
 * A dataset key {@link Type} and a {@link TimeReference} type are required.  A
 * {@link TimeReferenceGenerator} instance must be provided unless the
 * {@code TimeReference} type is one of the following:
 * <ul>
 *   <li>{@link FixedTimeReference}</li>
 *   <li>{@link SystemTimeReference}</li>
 * </ul>
 * If one of the above types is specified, a suitable {@code TimeReferenceGenerator} will
 * be supplied.
 *
 * @author Alex Snaps
 */
public final class SovereignBuilder<K extends Comparable<K>, Z extends TimeReference<Z>> {
  private final SovereignDataSetConfig<K, Z> config;

  public SovereignBuilder(final Type<K> keyType, final Class<Z> timeReferenceType) {
    Objects.requireNonNull(keyType, "keyType");
    Objects.requireNonNull(timeReferenceType, "timeReferenceType");
    this.config = new SovereignDataSetConfig<>(keyType, timeReferenceType);
  }

  public SovereignBuilder(final SovereignDataSetConfig<K, Z> config) {
    Objects.requireNonNull(config, "config");
    this.config = config.duplicate();

    if (this.config.getType() == null) {
      throw new IllegalStateException("Configuration does not specify a dataset key type");
    }

    if (this.config.getTimeReferenceGenerator() == null) {
      throw new IllegalStateException("Configuration does not specify a TimeReferenceGenerator instance");
    }
  }

  /**
   * Constructs a new {@link SovereignDataset} using the configuration assembled.
   *
   * @return a new {@code SovereignDataset} instance
   *
   * @throws IllegalStateException if a required configuration element was not provided
   */
  public SovereignDataset<K> build() {
    CachingSequence seq = new CachingSequence();
    // make it usable
    seq.setCallback(r -> {
    });

    // PersistableTimeReferenceGenerator.setPersistenceCallback is intentionally not called here.

    return new SovereignDatasetImpl<>(this.config, UUID.randomUUID(), seq);
  }

  public SovereignBuilder<K, Z> concurrency(final int concurrency) {
    config.concurrency(concurrency);
    return this;
  }

  public SovereignBuilder<K, Z> alias(final String alias) {
    config.alias(alias);
    return this;
  }

  public SovereignBuilder<K, Z> offheapResourceName(final String offheapResourceName) {
    config.offheapResourceName(offheapResourceName);
    return this;
  }

  public SovereignBuilder<K, Z> offheap(final long size) {
    config.resourceSize(SovereignDataSetConfig.StorageType.OFFHEAP, size);
    return this;
  }

  public SovereignBuilder<K, Z> offheap() {
    config.resourceSize(SovereignDataSetConfig.StorageType.OFFHEAP, 0);
    return this;
  }

  public SovereignBuilder<K, Z> heap() {
    config.resourceSize(SovereignDataSetConfig.StorageType.HEAP, 0);
    return this;
  }

  public SovereignBuilder<K, Z> heap(final long size) {
    config.resourceSize(SovereignDataSetConfig.StorageType.HEAP, size);
    return this;
  }

  public SovereignBuilder<K, Z> limitVersionsTo(final int i) {
    config.versionLimit(i);
    return this;
  }

  public SovereignBuilder<K, Z> versionLimitStrategy(final VersionLimitStrategy<?> versionLimitStrategy) {
    config.versionLimitStrategy(versionLimitStrategy);
    return this;
  }

  public SovereignBuilder<K, Z> recordLockTimeout(final long duration, final TimeUnit units) {
    Objects.requireNonNull(units, "units");
    config.recordLockTimeout(duration, units);
    return this;
  }

  public SovereignDataSetConfig<K, Z> getConfig() {
    return config.duplicate();
  }

  public SovereignBuilder<K, Z> fairLocking(final boolean fairLocking) {
    config.fairLocking(fairLocking);
    return this;
  }

  /**
   * Sets the {@link TimeReferenceGenerator} instance to use with the configuration
   * built by this {@code SovereignBuilder}.
   *
   * @param timeReferenceGenerator the non-{@code null} {@code TimeReferenceGenerator} instance;
   *                               must generate the {@link TimeReference} type specified in
   *                               {@link #SovereignBuilder(Type, Class)} or in the configuration
   *                               provided to {@link #SovereignBuilder(SovereignDataSetConfig)}.
   *
   * @return {@code this} {@code SovereignBuilder}
   *
   * @throws IllegalArgumentException if {@code timeReferenceGenerator} does not produce the expected
   *      {@code TimeReference} type
   */
  public SovereignBuilder<K, Z> timeReferenceGenerator(final TimeReferenceGenerator<Z> timeReferenceGenerator) {
    Objects.requireNonNull(timeReferenceGenerator, "timeReferenceGenerator");
    config.timeReferenceGenerator(timeReferenceGenerator);
    return this;
  }

  public SovereignBuilder<K, Z> storage(SovereignStorage<?, ?> storage) {
    config.storage((AbstractStorage)storage);
    return this;
  }

  // Please ignore this unless you really need to....
  public SovereignBuilder<K, Z> bufferStrategy(SovereignDataSetConfig.RecordBufferStrategyType rtype) {
    config.bufferStrategyType(rtype);
    return this;
  }

  public SovereignBuilder<K, Z> bufferStrategyLazy() {
    config.bufferStrategyType(SovereignDataSetConfig.RecordBufferStrategyType.VPLazy);
    return this;
  }

  public SovereignBuilder<K, Z> bufferStrategy() {
    config.bufferStrategyType(SovereignDataSetConfig.RecordBufferStrategyType.VP);
    return this;
  }

  public SovereignBuilder<K, Z> diskDurability(SovereignDatasetDiskDurability dur) {
    Objects.requireNonNull(dur);
    config.diskDurability(dur);
    return this;
  }


}
