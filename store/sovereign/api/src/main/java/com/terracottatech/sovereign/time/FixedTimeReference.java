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
import java.util.Objects;

/**
 * A {@code TimeReference} implementation for which all time references are equal.
 * <p>
 * The {@code FixedTimeReference} may be used in applications where time references
 * and time ordering are not relevant.
 *
 * @author Clifford W. Johnson
 */
public final class FixedTimeReference implements TimeReference<FixedTimeReference> {

  /**
   * Private, niladic constructor to prevent instantiation.
   * Use {@link Generator#get()} to obtain an instance.
   */
  private FixedTimeReference() { }

  /**
   * Compares {@code this} {@code FixedTimeReference} with another {@code FixedTimeReference}.
   * Since all {@code FixedTimeReference} instances are equals, zero is returned.
   *
   * @return 0 - all {@code FixedTimeReference} instances are equal
   *
   * @throws NullPointerException {@inheritDoc}
   */
  @Override
  public int compareTo(final TimeReference<?> t) {
    Objects.requireNonNull(t);
    return 0;       // All FixedTimeReference instances are 'equal'
  }

  @Override
  public boolean equals(final Object obj) {
    return this == obj || !(obj == null || getClass() != obj.getClass());
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return "FixedTimeReference{}";
  }

  /**
   * The {@link TimeReferenceGenerator} for the {@link FixedTimeReference} implementation.
   * <p>
   * This class produces an single {@code FixedTimeReference} instance.
   */
  public static final class Generator implements
      TimeReferenceGenerator<FixedTimeReference> {
    private static final long serialVersionUID = -936916769869040603L;
    private static final FixedTimeReference INSTANCE = new FixedTimeReference();

    @Override
    public Class<FixedTimeReference> type() {
      return FixedTimeReference.class;
    }

    @Override
    public FixedTimeReference get() {
      return INSTANCE;
    }

    @Override
    public int maxSerializedLength() {
      return 0;
    }

    @Override
    public void put(final ByteBuffer buffer, final TimeReference<?> timeReference) throws IOException {
      Objects.requireNonNull(buffer, "buffer");
      // No serialized representation
    }

    @Override
    public FixedTimeReference get(final ByteBuffer buffer) throws IOException, ClassNotFoundException {
      return INSTANCE;
    }
  }
}
