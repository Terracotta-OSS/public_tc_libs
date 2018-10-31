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

import org.junit.Test;

import com.terracottatech.sovereign.impl.SovereignType;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Tests operation of the {@link TimeReferenceCodec}.
 *
 * @author Clifford W. Johnson
 */
public class TimeReferenceCodecTest {

  /**
   * Tests {@code TimeReferenceCodec} operation over the {@link SystemTimeReference} implementation.
   */
  @Test
  public void testSystemTimeReference() throws Exception {
    final TimeReferenceGenerator<SystemTimeReference> generator = new SystemTimeReference.Generator();
    final TimeReferenceCodec codec = new TimeReferenceCodec(generator);
    final TimeReferenceCodec.Sizing sizer = new TimeReferenceCodec.Sizing(codec);
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    final SystemTimeReference timeReference = generator.get();

    assertThat(codec.getMaxSerializedLength(), is(generator.maxSerializedLength()));

    final CellDescriptor descriptor = codec.encode(buffer, timeReference);
    final int encodedSize = buffer.position();
    assertThat(encodedSize, is(0));
    assertThat(descriptor.cellFlags, is(0));
    assertThat(descriptor.nameDisplacement, is(0));
    assertThat(descriptor.valueType, is(SovereignType.LONG));
    assertThat(descriptor.valueDisplacement, is(timeReference.getTime()));

    final SystemTimeReference decoded = (SystemTimeReference) codec.decode(buffer, descriptor);
    assertThat(decoded, is(timeReference));
    assertThat(buffer.position(), is(0));

    assertThat(sizer.calculateSize(timeReference), is(encodedSize));
  }

  /**
   * Tests {@code TimeReferenceCodec} operation over the {@link FixedTimeReference} implementation.
   */
  @Test
  public void testFixedTimeReference() throws Exception {
    final TimeReferenceGenerator<FixedTimeReference> generator = new FixedTimeReference.Generator();
    final TimeReferenceCodec codec = new TimeReferenceCodec(generator);
    final TimeReferenceCodec.Sizing sizer = new TimeReferenceCodec.Sizing(codec);
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    final FixedTimeReference timeReference = generator.get();

    assertThat(codec.getMaxSerializedLength(), is(generator.maxSerializedLength()));

    final CellDescriptor descriptor = codec.encode(buffer, timeReference);
    final int encodedSize = buffer.position();
    assertThat(encodedSize, is(0));
    assertThat(descriptor.cellFlags, is(0));
    assertThat(descriptor.nameDisplacement, is(0));
    assertThat(descriptor.valueType, is(SovereignType.LONG));
    assertThat(descriptor.valueDisplacement, is(0L));

    final FixedTimeReference decoded = (FixedTimeReference) codec.decode(buffer, descriptor);
    assertThat(decoded, is(timeReference));
    assertThat(buffer.position(), is(0));

    assertThat(sizer.calculateSize(timeReference), is(encodedSize));
  }

  /**
   * Tests {@code TimeReferenceCodec} operation over an application-provided {@link TimeReference}
   * implementation using default serialization.
   */
  @Test
  public void testSerializableTestTimeReference() throws Exception {
    final TimeReferenceGenerator<SerializableTestTimeReference> generator = new SerializableTestTimeReference.Generator();
    final TimeReferenceCodec codec = new TimeReferenceCodec(generator);
    final TimeReferenceCodec.Sizing sizer = new TimeReferenceCodec.Sizing(codec);
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    final SerializableTestTimeReference firstTimeReference = generator.get();
    SerializableTestTimeReference secondTimeReference;
    do {
      secondTimeReference = generator.get();
    } while (secondTimeReference.compareTo(firstTimeReference) == 0);

    assertThat(codec.getMaxSerializedLength(), is(generator.maxSerializedLength()));

    final CellDescriptor firstDescriptor = codec.encode(buffer, firstTimeReference);
    final int encodedSize = buffer.position();
    assertThat(encodedSize, is(4 + codec.getMaxSerializedLength()));
    assertThat(firstDescriptor.cellFlags, is(0));
    assertThat(firstDescriptor.nameDisplacement, is(0));
    assertThat(firstDescriptor.valueType, is(SovereignType.BYTES));
    assertThat(firstDescriptor.valueDisplacement, is(0L));

    buffer.putInt(0xDEADBEEF);    // Arbitrary content between time reference values
    final int secondDisplacement = buffer.position();

    final CellDescriptor secondDescriptor = codec.encode(buffer, secondTimeReference);
    assertThat(secondDescriptor.cellFlags, is(0));
    assertThat(secondDescriptor.nameDisplacement, is(0));
    assertThat(secondDescriptor.valueType, is(SovereignType.BYTES));
    assertThat(secondDescriptor.valueDisplacement, is((long)secondDisplacement << 32));

    buffer.rewind();

    final SerializableTestTimeReference firstDecoded = (SerializableTestTimeReference) codec.decode(buffer, firstDescriptor);
    assertThat(firstDecoded, is(firstTimeReference));
    assertThat(buffer.position(), is(0));

    final SerializableTestTimeReference secondDecoded = (SerializableTestTimeReference) codec.decode(buffer, secondDescriptor);
    assertThat(secondDecoded, is(secondTimeReference));

    assertThat(sizer.calculateSize(firstTimeReference), is(encodedSize));
  }

  /**
   * Tests {@code TimeReferenceCodec} operation over an application-provided {@link TimeReference}
   * implementation using serialization provided by overrides of the
   * {@link TimeReferenceGenerator#put(ByteBuffer, TimeReference)} and
   * {@link TimeReferenceGenerator#get(ByteBuffer)} methods.
   */
  @Test
  public void testTestTimeReference() throws Exception {
    final TimeReferenceGenerator<TestTimeReference> generator = new TestTimeReference.Generator();
    final TimeReferenceCodec codec = new TimeReferenceCodec(generator);
    final TimeReferenceCodec.Sizing sizer = new TimeReferenceCodec.Sizing(codec);
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    final TestTimeReference timeReference = generator.get();

    assertThat(codec.getMaxSerializedLength(), is(generator.maxSerializedLength()));

    final CellDescriptor descriptor = codec.encode(buffer, timeReference);
    final int encodedSize = buffer.position();
    assertThat(encodedSize, is(4 + codec.getMaxSerializedLength()));
    assertThat(descriptor.cellFlags, is(0));
    assertThat(descriptor.nameDisplacement, is(0));
    assertThat(descriptor.valueType, is(SovereignType.BYTES));
    assertThat(descriptor.valueDisplacement, is(0L));

    buffer.rewind();

    final TestTimeReference decoded = (TestTimeReference) codec.decode(buffer, descriptor);
    assertThat(decoded, is(timeReference));
    assertThat(buffer.position(), is(0));

    assertThat(sizer.calculateSize(timeReference), is(encodedSize));
  }

  /**
   * Sample application-provided {@link TimeReference} implementation using serialization provided
   * by {@link TimeReferenceGenerator} methods.
   */
  private static final class TestTimeReference implements TimeReference<TestTimeReference> {
    private static final Comparator<TestTimeReference> COMPARATOR = Comparator.comparing(t -> t.timeReference);

    private final BigInteger timeReference;

    private TestTimeReference(final BigInteger timeReference) {
      this.timeReference = timeReference;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compareTo(final TimeReference t) {
      return COMPARATOR.compare(this, (TestTimeReference) t);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      final TestTimeReference that = (TestTimeReference)o;
      return Objects.equals(timeReference, that.timeReference);
    }

    @Override
    public int hashCode() {
      return Objects.hash(timeReference);
    }

    /**
     * The {@link TimeReferenceGenerator} implementation for {@link TimeReferenceCodecTest.TestTimeReference}.
     */
    private static class Generator implements TimeReferenceGenerator<TestTimeReference> {
      private static final long serialVersionUID = -8536475299443671881L;
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
      public Class<TestTimeReference> type() {
        return TestTimeReference.class;
      }

      @Override
      public TestTimeReference get() {
        return new TestTimeReference(this.origin.add(BigInteger.valueOf(System.nanoTime() - this.nanoOrigin)));
      }

      @Override
      public int maxSerializedLength() {
        return 0;
      }

      @Override
      public void put(final ByteBuffer buffer, final TimeReference<?> timeReference) throws IOException {
        final byte[] bytes = ((TestTimeReference)timeReference).timeReference.toByteArray();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
      }

      @Override
      public TestTimeReference get(final ByteBuffer buffer)
          throws IOException, ClassNotFoundException {
        final byte[] bytes = new byte[buffer.getInt()];
        buffer.get(bytes);
        return new TestTimeReference(new BigInteger(bytes));
      }
    }
  }
}
