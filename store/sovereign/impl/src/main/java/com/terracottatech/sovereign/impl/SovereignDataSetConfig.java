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

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.VersionLimitStrategy;
import com.terracottatech.sovereign.exceptions.InvalidConfigException;
import com.terracottatech.sovereign.impl.persistence.AbstractPersistentStorage;
import com.terracottatech.sovereign.impl.persistence.AbstractStorage;
import com.terracottatech.sovereign.impl.persistence.StorageTransient;
import com.terracottatech.sovereign.impl.utils.TriPredicate;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.store.Type;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * @author cschanck
 */
public class SovereignDataSetConfig<K extends Comparable<K>, Z extends TimeReference<Z>> implements
  Serializable {

  private static final long serialVersionUID = 2L;
  public static final int DEFAULT_MAX_RESOURCE_SIZE = 128 * 1024 * 1024;
  public static final int DEFAULT_MIN_RESOURCE_SIZE = 1024 * 1024;

  public enum RecordBufferStrategyType {
    Versioned,
    Simple,
    VP,
    VPLazy
  }

  public enum StorageType {
    HEAP,
    OFFHEAP
  }

  private final Class<K> clz;
  private final Class<Z> timeReferenceType;

  private transient volatile boolean frozen = false;
  private transient TimeReferenceGenerator<Z> timeReferenceGenerator = null;
  private transient AbstractStorage storage = new StorageTransient(SovereignBufferResource.unlimited());
  private String alias;
  private String offheapResourceName;
  private long resourceSize = 0;
  private StorageType storageType = SovereignDataSetConfig.StorageType.HEAP;
  private RecordBufferStrategyType bufferStrategyType = RecordBufferStrategyType.VPLazy;
  private VersionLimitStrategy<?> versionLimitStrategy;
  private int versionLimit = 1;
  private boolean fairLocking = true;
  private long recordLockTimeout = TimeUnit.MINUTES.toMillis(2L);
  private ConcurrencyFormulator<K> concurrencyFormulator;
  private SovereignDatasetDiskDurability diskDurability = null;

  public SovereignDataSetConfig(final Type<K> keyType, final Class<Z> timeReferenceType) {
    Objects.requireNonNull(keyType, "keyType");
    Objects.requireNonNull(timeReferenceType, "timeReferenceType");

    if (!Comparable.class.isAssignableFrom(keyType.getJDKType())) {
      throw new InvalidConfigException(keyType.toString() + " is not an accepted key type");
    }
    this.clz = keyType.getJDKType();
    this.concurrencyFormulator = new ConcurrencyFormulator<>(keyType.getJDKType(), Optional.empty());

    this.timeReferenceType = timeReferenceType;
    this.setDefaultTimeReferenceGenerator();

    // Must be anonymous class to avoid lambda deserialization issue
    this.versionLimitStrategy = new VersionLimitStrategyImpl<>(SovereignDataSetConfig.this.timeReferenceType);
  }

  public SovereignDatasetDiskDurability getDiskDurability() {
    if (diskDurability == null) {
      if (storage instanceof AbstractPersistentStorage) {
        return ((AbstractPersistentStorage) storage).getDefaultDiskDurability();
      }
      return SovereignDatasetDiskDurability.NONE;
    }
    return diskDurability;
  }

  public SovereignDataSetConfig<K, Z> diskDurability(SovereignDatasetDiskDurability diskDurability) {
    this.diskDurability = diskDurability;
    return this;
  }

  public static class VersionLimitStrategyImpl<Z extends TimeReference<Z>> implements VersionLimitStrategy<Z> {
    private static final long serialVersionUID = 3594958010269126589L;

    private final Class<Z> timeReferenceType;

    public VersionLimitStrategyImpl(Class<Z> timeReferenceType) {
      this.timeReferenceType = timeReferenceType;
    }

    @Override
    public Class<Z> type() {
      return this.timeReferenceType;
    }
  }

  /**
   * Sets a default {@link TimeReferenceGenerator} instance, if supported, for the
   * {@link #timeReferenceType} configured.
   */
  public void setDefaultTimeReferenceGenerator() {
    if (this.timeReferenceType == SystemTimeReference.class) {
      this.applyTimeReferenceGenerator(new SystemTimeReference.Generator());
    } else if (this.timeReferenceType == FixedTimeReference.class) {
      this.applyTimeReferenceGenerator(new FixedTimeReference.Generator());
    }
  }


  @SuppressWarnings("unchecked")
  private void applyTimeReferenceGenerator(final TimeReferenceGenerator<?> timeReferenceGenerator) {
    this.timeReferenceGenerator = (TimeReferenceGenerator<Z>) timeReferenceGenerator;   // unchecked
  }

  public void validate() {
    if (storageType == null) {
      throw new InvalidConfigException("No storage type specified");
    }
    if (Type.forJdkType(clz) == null) {
      throw new InvalidConfigException("No key type specified.");
    }
    if (getConcurrency() <= 0 || Integer.bitCount(getConcurrency()) != 1) {
      throw new InvalidConfigException("Invalid concurrency specified: " + getConcurrency());
    }
    if (resourceSize < 0) {
      throw new InvalidConfigException("Invalid memory sizing specified: " + resourceSize);
    }
    if (versionLimit < 0) {
      throw new InvalidConfigException("Invalid version limit specified: " + versionLimit);
    }
    if (versionLimitStrategy == null) {
      throw new InvalidConfigException("No VersionLimitStrategy specified");
    }
    if (timeReferenceGenerator == null) {
      throw new InvalidConfigException("No TimeReferenceGenerator specified");
    }
    if (recordLockTimeout <= 0) {
      throw new InvalidConfigException("Invalid record lock timeout specified: " + recordLockTimeout);
    }
  }

  private void testFrozen() {
    if (frozen) {
      throw new IllegalStateException("Attempt to mutate a frozen config.");
    }
  }

  public void alias(String alias) {
    testFrozen();
    this.alias = alias;
  }

  public void offheapResourceName(String offheapResourceName) {
    testFrozen();
    this.offheapResourceName = offheapResourceName;
  }

  public Type<K> getType() {
    return Type.forJdkType(clz);
  }

  public int getConcurrency() {
    return concurrencyFormulator.getConcurrency();
  }

  public long getResourceSize() {
    return resourceSize;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  public int getVersionLimit() {
    return versionLimit;
  }

  public VersionLimitStrategy<?> getVersionLimitStrategy() {
    return this.versionLimitStrategy;
  }

  public TimeReferenceGenerator<Z> getTimeReferenceGenerator() {
    return this.timeReferenceGenerator;
  }

  @Deprecated
  // TODO: Remove after TCSTORE is updated
  public long getKeepInMillis() {
    return Integer.MAX_VALUE;
  }

  public AbstractStorage getStorage() {
    return storage;
  }

  public long getRecordLockTimeout() {
    return recordLockTimeout;
  }

  public SovereignDataSetConfig<K, Z> concurrency(final int concurrency) {
    testFrozen();
    this.concurrencyFormulator = new ConcurrencyFormulator<>(clz, Optional.of(concurrency));
    return this;
  }

  public SovereignDataSetConfig<K, Z> resourceSize(final StorageType type, final long resourceSize) {
    testFrozen();
    this.resourceSize = resourceSize;
    this.storageType = type;
    return this;
  }

  @SuppressWarnings("rawtypes")
  public SovereignDataSetConfig<K, Z> versionLimitStrategy(final VersionLimitStrategy versionLimitStrategy) {
    testFrozen();
    Objects.requireNonNull(versionLimitStrategy, "versionLimitStrategy");

    if (versionLimitStrategy.type() != this.timeReferenceType) {
      throw new IllegalArgumentException(
        "VersionLimitStrategy<" + this.timeReferenceType.getName() + "> expected; found VersionLimitStrategy<" + versionLimitStrategy.type().getName() + ">");
    }
    this.versionLimitStrategy = versionLimitStrategy;
    return this;
  }

  public SovereignDataSetConfig<K, Z> timeReferenceGenerator(final TimeReferenceGenerator<Z> timeReferenceGenerator) {
    Objects.requireNonNull(timeReferenceGenerator, "timeReferenceGenerator");

    if (timeReferenceGenerator.type() != this.timeReferenceType) {
      throw new IllegalArgumentException(
        "TimeReferenceGenerator<" + this.timeReferenceType.getName() + "> expected; found TimeReferenceGenerator<" + timeReferenceGenerator.type().getName() + ">");
    }
    this.timeReferenceGenerator = timeReferenceGenerator;
    return this;
  }

  public SovereignDataSetConfig<K, Z> versionLimit(final int limit) {
    testFrozen();
    this.versionLimit = limit;
    return this;
  }

  public String getAlias() {
    return alias;
  }

  public String getOffheapResourceName() {
    return offheapResourceName;
  }

  SovereignDataSetConfig<K, Z> duplicate() {
    SovereignDataSetConfig<K, Z> ret = new SovereignDataSetConfig<>(Type.forJdkType(clz), this.timeReferenceType);
    ret.resourceSize = resourceSize;
    ret.bufferStrategyType = bufferStrategyType;
    ret.storageType = storageType;
    ret.concurrencyFormulator = concurrencyFormulator;
    ret.alias = alias;
    ret.offheapResourceName = offheapResourceName;
    ret.fairLocking = fairLocking;
    ret.versionLimitStrategy = versionLimitStrategy;
    ret.timeReferenceGenerator = timeReferenceGenerator;
    ret.recordLockTimeout = recordLockTimeout;
    ret.versionLimit = versionLimit;
    ret.storage = storage;
    ret.diskDurability = diskDurability;
    return ret;
  }

  public int maxResourceChunkSize() {
    if (getResourceSize() > 0) {
      // between min and max
      int ret = (int) Math.max(DEFAULT_MIN_RESOURCE_SIZE, Math.min(getResourceSize(), DEFAULT_MAX_RESOURCE_SIZE));
      // make sure power of 2.
      if (Integer.bitCount(ret) != 1) {
        // next smallest power of two
        ret = Integer.highestOneBit(ret - 1);
      }
      return ret;
    } else {
      return DEFAULT_MAX_RESOURCE_SIZE;
    }
  }

  public boolean isFairLocking() {
    return fairLocking;
  }

  public SovereignDataSetConfig<K, Z> fairLocking(final boolean fairLocking) {
    testFrozen();
    this.fairLocking = fairLocking;
    return this;
  }

  public RecordBufferStrategyType getBufferStrategyType() {
    return bufferStrategyType;
  }

  public SovereignDataSetConfig<K, Z> bufferStrategyType(final RecordBufferStrategyType bufferStrategyType) {
    this.bufferStrategyType = bufferStrategyType;
    return this;
  }

  /**
   * Returns a {@code BiFunction} instance that determines if a {@link com.terracottatech.store.Record Record}
   * version should be retained in the version collection.
   * The arguments to the returned function are:
   * <ul>
   * <li>{@code now} - a {@code TimeReference} instance representing the <i>current</i> time (obtained
   * from the {@link TimeReferenceGenerator TimeReferenceGenerator} for
   * {@code <Z>})</li>
   * <li>{@code t} - the {@code TimeReference} from the {@code Record} version being examined</li>
   * </ul>
   * The function returns a {@link com.terracottatech.sovereign.VersionLimitStrategy.Retention Retention} value
   * indicating whether or not the version is retained in the updated or retrieved {@code Record}.
   *
   * @return a {@code Retention} value indicating if the handling of the record version
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public BiFunction<TimeReference<?>, TimeReference<?>, VersionLimitStrategy.Retention> makeRecordRetrievalFilter() {
    return (BiFunction<TimeReference<?>, TimeReference<?>, VersionLimitStrategy.Retention>) this.versionLimitStrategy.getTimeReferenceFilter();
  }

  /**
   * Returns a {@link TriPredicate} instance that, given the current {@link TimeReference} instance (now),
   * the {@link com.terracottatech.store.Record Record} version {@code TimeReference}, and the version
   * ordinal, determines if the version is retained in the {@code Record}.
   * <p/>
   * The predicate returned takes the following arguments:
   * <ul>
   * <li>{@code now} - a {@code TimeReference} instance representing now</li>
   * <li>{@code timeReference} - the {@code TimeReference} instance from the {@code Record} version being tested</li>
   * <li>{@code version} - the a number indicating the ordinal position, by creation order, of the {@code Record}
   * version being tested in the set of versions in the {@code Record} with {@code 0} indicating the newest version</li>
   * </ul>
   *
   * @return {@code true} if the version is to be retained; {@code false} if the version should be dropped
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public TriPredicate<TimeReference<?>, TimeReference<?>, Integer> makeRecordConstructionFilter() {
    final BiFunction<TimeReference, TimeReference, VersionLimitStrategy.Retention> timeReferenceFilter = (BiFunction<TimeReference, TimeReference, VersionLimitStrategy.Retention>) this
            .versionLimitStrategy
            .getTimeReferenceFilter();
    int versionLimit = getVersionLimit();
    return (now, timeReference, version) -> (versionLimit == 0 || version < versionLimit)
        && timeReferenceFilter.apply(now, timeReference) != VersionLimitStrategy.Retention.DROP;
  }

  public SovereignDataSetConfig<K, Z> freeze() {
    frozen = true;
    return this;
  }

  public SovereignDataSetConfig<K, Z> storage(final AbstractStorage storage) {
    testFrozen();
    this.storage = storage;
    return this;
  }

  public SovereignDataSetConfig<K, Z> recordLockTimeout(final long duration, final TimeUnit units) {
    testFrozen();
    Objects.requireNonNull(units, "units");
    if (duration < 0) {
      throw new IllegalArgumentException("Invalid record lock timeout specified: " + duration);
    }
    this.recordLockTimeout = units.toMillis(duration);
    return this;
  }

  @Override
  public String toString() {
    return "SovereignDataSetConfig{" +
      "alias='" + alias + '\'' +
      ", offheapResourceName=" + offheapResourceName +
      ", timeReferenceType=" + timeReferenceType +
      ", frozen=" + frozen +
      ", concurrency=" + getConcurrency() +
      ", resourceSize=" + resourceSize +
      ", storageType=" + storageType +
      ", timeReferenceGenerator=" + timeReferenceGenerator +
      ", versionLimitStrategy=" + versionLimitStrategy +
      ", versionLimit=" + versionLimit +
      ", storage=" + storage +
      ", fairLocking=" + fairLocking +
      ", recordLockTimeout=" + recordLockTimeout +
      ", diskDurability=" + diskDurability +
      '}';
  }


}
