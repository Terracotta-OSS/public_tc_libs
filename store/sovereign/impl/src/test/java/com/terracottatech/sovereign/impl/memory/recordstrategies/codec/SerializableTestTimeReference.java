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

package com.terracottatech.sovereign.impl.memory.recordstrategies.codec;

import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.Objects;

/**
 * Sample application-provided {@link TimeReference} implementation that uses default Java serialization.
 */
final class SerializableTestTimeReference implements TimeReference<SerializableTestTimeReference>, Serializable {
  private static final long serialVersionUID = 744421650933248180L;
  private static final Comparator<SerializableTestTimeReference> COMPARATOR =
      Comparator.comparing(t -> t.timeReference);

  private final BigInteger timeReference;

  private SerializableTestTimeReference(final BigInteger timeReference) {
    this.timeReference = timeReference;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compareTo(final TimeReference t) {
    return COMPARATOR.compare(this, (SerializableTestTimeReference) t);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final SerializableTestTimeReference that = (SerializableTestTimeReference)o;
    return Objects.equals(timeReference, that.timeReference);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeReference);
  }

  /**
   * The {@link TimeReferenceGenerator} implementation for {@link SerializableTestTimeReference}.
   */
  static final class Generator implements TimeReferenceGenerator<SerializableTestTimeReference> {
    private static final long serialVersionUID = 1013390484796104655L;
    private static final BigInteger NANOS_IN_MILLI = BigInteger.TEN.pow(6);

    /**
     * Initial nanosecond-resolution TOD.
     */
    private final BigInteger origin;

    /**
     * {@code System.nanoTime} value equated with {@link #origin}.
     */
    private final long nanoOrigin;

    public Generator() {
      long nanos = System.nanoTime();
      long tod = System.currentTimeMillis();
      this.nanoOrigin = nanos + (System.nanoTime() - nanos) / 2;
      this.origin = BigInteger.valueOf(tod).multiply(NANOS_IN_MILLI).add(BigInteger.valueOf(this.nanoOrigin % 1000000));
    }

    @Override
    public Class<SerializableTestTimeReference> type() {
      return SerializableTestTimeReference.class;
    }

    @Override
    public SerializableTestTimeReference get() {
      return new SerializableTestTimeReference(
          this.origin.add(BigInteger.valueOf(System.nanoTime() - this.nanoOrigin)));
    }

    @Override
    public int maxSerializedLength() {
      return 0;
    }
  }
}
