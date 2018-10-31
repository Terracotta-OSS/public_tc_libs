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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

/**
 * A {@code TimeReference} implementation using {@link System#currentTimeMillis()} as the time source.
 * <p>
 * {@code SystemTimeReference} instances are <b>not</b> guaranteed to be unique.  Although resolution
 * of time references based on {@code System.currentTimeMillis} is milliseconds, its precision is
 * dependent on the host operating system and environment and can vary significantly permitting
 * multiple {@code SystemTimeReference} instances to be created having the same time value.  It is also
 * possible for the instruction execution speed of the environment to permit multiple
 * {@code SystemTimeReference} instances to be created within the millisecond resolution of
 * {@code System.currentTimeMillis}.
 *
 * @author Clifford W. Johnson
 *
 * @see Generator
 */
public final class SystemTimeReference implements TimeReference<SystemTimeReference> {
  private static final Comparator<SystemTimeReference> COMPARATOR = Comparator.comparingLong(
      r -> r.systemTimeMillis);

  private final long systemTimeMillis;

  SystemTimeReference() {
    this.systemTimeMillis = System.currentTimeMillis();
  }

  private SystemTimeReference(final long systemTimeMillis) {
    this.systemTimeMillis = systemTimeMillis;
  }

  /**
   * Returns the time value, in milliseconds from epoch, for this {@code SystemTimeReference}.
   *
   * @return the time value
   */
  public long getTime() {
    return systemTimeMillis;
  }

  @Override
  public int compareTo(final TimeReference<?> t) {
    return COMPARATOR.compare(this, (SystemTimeReference) t);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SystemTimeReference that = (SystemTimeReference)o;
    return (this.systemTimeMillis == that.systemTimeMillis);
  }

  @Override
  public int hashCode() {
    return Objects.hash(systemTimeMillis);
  }

  @Override
  public String toString() {
    return "SystemTimeReference{" +
        "systemTimeMillis=" + systemTimeMillis +
        '}';
  }

  /**
   * The {@link TimeReferenceGenerator} for {@link SystemTimeReference SystemTimeReference}.
   */
  public static final class Generator implements
      TimeReferenceGenerator<SystemTimeReference> {
    private static final long serialVersionUID = 2171973690835358313L;

    @Override
    public Class<SystemTimeReference> type() {
      return SystemTimeReference.class;
    }

    @Override
    public SystemTimeReference get() {
      return new SystemTimeReference();
    }

    @Override
    public int maxSerializedLength() {
      return 8;
    }

    @Override
    public void put(final ByteBuffer buffer, final TimeReference<?> timeReference) throws IOException {
      Objects.requireNonNull(buffer, "buffer");
      Objects.requireNonNull(timeReference, "timeReference");
      buffer.putLong(((SystemTimeReference)timeReference).systemTimeMillis);
    }

    @Override
    public SystemTimeReference get(final ByteBuffer buffer) throws IOException, ClassNotFoundException {
      Objects.requireNonNull(buffer, "buffer");
      return new SystemTimeReference(buffer.getLong());
    }
  }
}
