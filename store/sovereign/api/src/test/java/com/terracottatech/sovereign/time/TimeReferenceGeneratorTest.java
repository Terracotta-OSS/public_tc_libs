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

import org.junit.Test;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

/**
 * Tests for the default methods in the {@link TimeReferenceGenerator} interface.
 *
 * @author Clifford W. Johnson
 */
public class TimeReferenceGeneratorTest {

  /**
   * Ensures that {@code TimeReferenceGenerator.put(null, timeReference)} throws a {@code NullPointerException}.
   */
  @Test
  public void testDefaultPutNullBuffer() throws Exception {
    final BasicTimeReferenceGenerator timeReferenceGenerator = new BasicTimeReferenceGenerator();
    final BasicTimeReference timeReference = timeReferenceGenerator.get();

    try {
      timeReferenceGenerator.put(null, timeReference);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Ensures that {@code TimeReferenceGenerator.put(buffer, null)} works correctly.
   */
  @Test
  public void testDefaultPutNullTimeReference() throws Exception {
    final BasicTimeReferenceGenerator timeReferenceGenerator = new BasicTimeReferenceGenerator();
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    timeReferenceGenerator.put(buffer, null);
    buffer.flip();

    final BasicTimeReference deserializedTimeReference =
        timeReferenceGenerator.get(buffer);

    assertThat(deserializedTimeReference, is(nullValue()));
  }

  /**
   * Ensures that {@code TimeReferenceGenerator.get(null)} throws a {@code NullPointerException}.
   */
  @Test
  public void testDefaultGetNullBuffer() throws Exception {
    final BasicTimeReferenceGenerator timeReferenceGenerator = new BasicTimeReferenceGenerator();

    try {
      timeReferenceGenerator.get(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Ensures that {@link TimeReferenceGenerator#put(ByteBuffer, TimeReference)} and
   * {@link TimeReferenceGenerator#get(ByteBuffer)} can <i>round-trip</i> a
   * {@link TimeReference}.
   */
  @Test
  public void testDefaultPutGet() throws Exception {
    final BasicTimeReferenceGenerator timeReferenceGenerator = new BasicTimeReferenceGenerator();
    final BasicTimeReference originalTimeReference = timeReferenceGenerator.get();
    assertNotNull(originalTimeReference);

    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    final int originalPosition = buffer.position();
    timeReferenceGenerator.put(buffer, originalTimeReference);
    assertThat(buffer.position(), not(equalTo(originalPosition)));

    buffer.flip();

    final BasicTimeReference deserializedTimeReference =
        timeReferenceGenerator.get(buffer);

    assertThat(deserializedTimeReference, comparesEqualTo(originalTimeReference));

    final BasicTimeReference nextTimeReference = timeReferenceGenerator.get();
    assertThat(nextTimeReference, greaterThan(originalTimeReference));

    buffer.clear();

    timeReferenceGenerator.put(buffer, nextTimeReference);
    buffer.flip();
    final BasicTimeReference deserializedNextTimeReference =
        timeReferenceGenerator.get(buffer);

    assertThat(deserializedNextTimeReference, comparesEqualTo(nextTimeReference));
  }

  /**
   * Simple {@link TimeReferenceGenerator} implementation that <b>retains</b> the default
   * implementations of {@link #put(ByteBuffer, TimeReference)} and {@link TimeReferenceGenerator#get(ByteBuffer)}.
   */
  private static final class BasicTimeReferenceGenerator implements TimeReferenceGenerator<BasicTimeReference> {
    private static final long serialVersionUID = 6345766374534655216L;

    private final AtomicLong timeSource = new AtomicLong(0);

    @Override
    public Class<BasicTimeReference> type() {
      return BasicTimeReference.class;
    }

    @Override
    public BasicTimeReference get() {
      return new BasicTimeReference(BigInteger.valueOf(timeSource.incrementAndGet()));
    }

    @Override
    public int maxSerializedLength() {
      return 0;
    }
  }

  /**
   * Rudimentary serializable {@link TimeReference} implementation.
   */
  private static final class BasicTimeReference implements TimeReference<BasicTimeReference>, Serializable {
    private static final long serialVersionUID = 4314520403823834960L;

    private static final Comparator<BasicTimeReference> TIME_REFERENCE_COMPARATOR = Comparator.comparing(tr -> tr.time);

    private final BigInteger time;

    private BasicTimeReference(final BigInteger time) {
      this.time = time;
    }

    public int compareTo(final TimeReference<?> t) {
      return TIME_REFERENCE_COMPARATOR.compare(this, (BasicTimeReference) t);
    }
  }
}