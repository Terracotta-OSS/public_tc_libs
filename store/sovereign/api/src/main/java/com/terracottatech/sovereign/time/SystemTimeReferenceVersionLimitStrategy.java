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
package com.terracottatech.sovereign.time;

import com.terracottatech.sovereign.VersionLimitStrategy;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * A {@link VersionLimitStrategy} implementation for datasets using {@link SystemTimeReference} as
 * a record time reference.
 *
 * @author Clifford W. Johnson
 */
public class SystemTimeReferenceVersionLimitStrategy implements VersionLimitStrategy<SystemTimeReference> {
  private static final long serialVersionUID = -3419801499743334650L;

  private final long keepInMillis;

  /**
   * Constructs a new {@code SystemTimeReferenceVersionLimitStrategy} instance.
   *
   * @param time the non-negative version retention time; record versions older than this
   *             value will not be returned to {@code SovereignDataset} retrieval methods
   *             and are subject to removal from the dataset
   * @param unit the units for {@code time}; must not be {@code null}
   */
  public SystemTimeReferenceVersionLimitStrategy(final long time, final TimeUnit unit) {
    Objects.requireNonNull(unit, "unit");
    if (time < 0) {
      throw new IllegalArgumentException("time must be non-negative");
    }
    final long millis = unit.toMillis(time);
    if (time != 0 && millis == 0) {
      throw new IllegalArgumentException("non-zero time must be at least 1 millisecond");
    }
    this.keepInMillis = millis;
  }

  @Override
  public Class<SystemTimeReference> type() {
    return SystemTimeReference.class;
  }

  /**
   * Gets the time, in milliseconds, a version is kept before being subject to dropping.
   *
   * @return the keep time in milliseconds
   */
  long getKeepInMillis() {
    return keepInMillis;
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation drops versions for which  {@code currentTime - versionTime > time}
   * is {@code true}.
   *
   * @return {@inheritDoc}
   */
  @Override
  public BiFunction<SystemTimeReference, SystemTimeReference, Retention> getTimeReferenceFilter() {
    if (this.keepInMillis == 0) {
      return (now, t) -> Retention.FINISH;
    }

    return (now, t) -> {
      if (now.getTime() - t.getTime() > this.keepInMillis) {
        return Retention.DROP;
      }
      return Retention.FINISH;
    };
  }

  @Override
  public String toString() {
    return "SystemTimeReferenceVersionLimitStrategy{" +
        "keepInMillis=" + this.keepInMillis +
        '}';
  }
}
