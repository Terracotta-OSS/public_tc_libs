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
import com.terracottatech.sovereign.impl.memory.SingleRecord;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.store.Cell;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

/**
 * Provides tests for {@link ValuePool}, {@link ValuePoolReader}, and {@link ValuePoolWriter}.
 *
 * @author Clifford W. Johnson
 */
public class ValuePoolTest {

  private static final String SMALL_STRING_VALUE = "username";
  private static final String LARGE_STRING_VALUE;
  static {
    final Random rnd = new Random(8675309L);
    final StringBuilder sb = new StringBuilder(65536 * 2);
    final int capacity = sb.capacity();
    while (sb.length() < capacity) {
      sb.appendCodePoint(rnd.nextInt(1 + Character.MAX_CODE_POINT));
    }
    LARGE_STRING_VALUE = sb.toString();
  }
  private static final Boolean BOOL_VALUE = Boolean.TRUE;
  private static final Character CHAR_VALUE = 'a';
  private static final Integer INT_VALUE = 8675309;
  private static final Long LONG_VALUE = 8675309L << 24;
  private static final Double DOUBLE_VALUE = Math.PI;
  private static final byte[] BYTES_VALUE =
      new byte[] {(byte)0xDE,(byte)0xAD,(byte)0xBE,(byte)0xEF,(byte)0xCA,(byte)0xFE,(byte)0xBA,(byte)0xBE};

  private final FixedTimeReference.Generator generator = new FixedTimeReference.Generator();
  private final TimeReferenceCodec codec = new TimeReferenceCodec(generator);

  @Test
  public void testCtorWritableBad() throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    try {
      new ValuePool(null, ValuePool.getSerializedSize(), this.codec);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    try {
      new ValuePool(buffer, ValuePool.getSerializedSize(), null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    try {
      new ValuePool(buffer, ValuePool.getSerializedSize() - 1, this.codec);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new ValuePool(ByteBuffer.allocate(0), ValuePool.getSerializedSize(), this.codec);
      fail();
    } catch (BufferOverflowException e) {
      // expected
    }
  }

  @Test
  public void testCtorReadableBad() throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    try {
      new ValuePool(ByteBuffer.allocate(ValuePool.getSerializedSize() - 1), this.codec);
      fail();
    } catch (BufferUnderflowException e) {
      // expected
    }

    try {
      new ValuePool(buffer, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testCtorWritableZeroLength() throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    final ValuePool valuePool = new ValuePool(buffer, ValuePool.getSerializedSize(), this.codec);

    assertThat(buffer.position(), is(ValuePool.getSerializedSize()));

    final ValuePoolWriter poolWriter = valuePool.getPoolWriter();
    assertThat(poolWriter, is(not(nullValue())));

    try {
      valuePool.getPoolReader();
      fail();
    } catch (IllegalStateException e) {
      // expected
    }

    try {
      poolWriter.writeValue(SovereignType.BYTES, new byte[0]);
      fail();
    } catch (BufferOverflowException e) {
      // expected
    }
  }

  @Test
  public void testCtorReadableZeroLength() throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    // An empty buffer is a valid (zero-length) ValuePool as long as it's long enough
    final ValuePool valuePool = new ValuePool(buffer, this.codec);

    assertThat(buffer.position(), is(ValuePool.getSerializedSize()));

    final ValuePoolReader poolReader = valuePool.getPoolReader();
    assertThat(poolReader, is(not(nullValue())));

    try {
      valuePool.getPoolWriter();
      fail();
    } catch (IllegalStateException e) {
      // expected
    }

    try {
      poolReader.readValue(SovereignType.BYTES, 0L);
      fail();
    } catch (BufferUnderflowException e) {
      // expected
    }
  }

  @Test
  public void testBoolean() throws Exception {
    testPrimitiveValue(SovereignType.BOOLEAN, BOOL_VALUE, 0x0100000000000000L);
    testPrimitiveKey(BOOL_VALUE, 0x0100000000000000L);

    testCell(Cell.cell("boolean", BOOL_VALUE));
  }

  @Test
  public void testChar() throws Exception {
    testPrimitiveValue(SovereignType.CHAR, CHAR_VALUE, (long)CHAR_VALUE << 48);
    testPrimitiveKey(CHAR_VALUE, (long)CHAR_VALUE << 48);

    testCell(Cell.cell("char", CHAR_VALUE));
  }

  @Test
  public void testInt() throws Exception {
    testPrimitiveValue(SovereignType.INT, INT_VALUE, (long)INT_VALUE << 32);
    testPrimitiveKey(INT_VALUE, (long)INT_VALUE << 32);

    testCell(Cell.cell("int", INT_VALUE));
  }

  @Test
  public void testLong() throws Exception {
    testPrimitiveValue(SovereignType.LONG, LONG_VALUE, LONG_VALUE);
    testPrimitiveKey(LONG_VALUE, LONG_VALUE);

    testCell(Cell.cell("long", LONG_VALUE));
  }

  @Test
  public void testDouble() throws Exception {
    testPrimitiveValue(SovereignType.DOUBLE, DOUBLE_VALUE, Double.doubleToRawLongBits(DOUBLE_VALUE));
    testPrimitiveKey(DOUBLE_VALUE, Double.doubleToRawLongBits(DOUBLE_VALUE));

    testCell(Cell.cell("double", DOUBLE_VALUE));
  }

  @Test
  public void testShortString() throws Exception {
    testNonPrimitiveValue(SovereignType.STRING, SMALL_STRING_VALUE);
    testNonPrimitiveKey(SMALL_STRING_VALUE);

    testCell(Cell.cell("string", SMALL_STRING_VALUE));
  }

  @Test
  public void testLargeString() throws Exception {
    testNonPrimitiveValue(SovereignType.STRING, LARGE_STRING_VALUE);
    testNonPrimitiveKey(LARGE_STRING_VALUE);

    testCell(Cell.cell("string", LARGE_STRING_VALUE));
  }

  @Test
  public void testBytes() throws Exception {
    testNonPrimitiveValue(SovereignType.BYTES, BYTES_VALUE);

    testCell(Cell.cell("bytes", BYTES_VALUE));
  }

  @Test
  public void testMulti() throws Exception {
    final TimeReferenceGenerator<SerializableTestTimeReference> generator =
        new SerializableTestTimeReference.Generator();
    final TimeReferenceCodec codec = new TimeReferenceCodec(generator);

    final SingleRecord<String> record =
        new SingleRecord<>(null, SMALL_STRING_VALUE, generator.get(), 1L,
            Cell.cell("boolean", BOOL_VALUE),
            Cell.cell("char", CHAR_VALUE),
            Cell.cell("double", DOUBLE_VALUE),
            Cell.cell("int", INT_VALUE),
            Cell.cell("long", LONG_VALUE),
            Cell.cell("string", LARGE_STRING_VALUE),
            Cell.cell("bytes", BYTES_VALUE));
    final int poolSize = ValuePool.calculatePoolSize(record, codec);
    final ByteBuffer buffer = ByteBuffer.allocate(poolSize);

    final ValuePool writableValuePool = new ValuePool(buffer, poolSize, codec);
    final ValuePoolWriter poolWriter = writableValuePool.getPoolWriter();

    final CellDescriptor keyDescriptor = poolWriter.writeKey(record.getKey());
    final CellDescriptor timeReferenceDescriptor = poolWriter.writeTimeReference(record.getTimeReference());
    final Map<Cell<?>, CellDescriptor> cellDescriptors = new HashMap<>();
    for (final Cell<?> cell : record.cells().values()) {
      cellDescriptors.put(cell, poolWriter.writeCell(cell));
    }

    assertThat(poolWriter.position() + ValuePool.getSerializedSize(), is(poolSize));

    buffer.rewind();

    final ValuePool readableValuePool = new ValuePool(buffer, codec);
    final ValuePoolReader poolReader = readableValuePool.getPoolReader();

    assertThat(poolReader.readKey(keyDescriptor), is(equalTo(record.getKey())));
    assertThat(poolReader.readTimeReference(timeReferenceDescriptor), is(equalTo(record.getTimeReference())));
    for (Map.Entry<Cell<?>, CellDescriptor> entry : cellDescriptors.entrySet()) {
      assertThat(poolReader.readCell(entry.getValue()), is(equalTo(entry.getKey())));
    }
  }

  /**
   * Exercises {@link ValuePoolWriter} and {@link ValuePoolReader} for Sovereign primitive types.  Primitive type
   * values fit into the {@link CellDescriptor#valueDisplacement} field.
   *
   * @param testType the Sovereign type to test
   * @param testValue the test value to use
   * @param expectedDisplacement the expected primitive value encoding
   */
  private void testPrimitiveValue(final SovereignType testType, final Object testValue, final long expectedDisplacement)
      throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    final ValuePool writableValuePool =
        new ValuePool(buffer, buffer.capacity(), this.codec);
    final ValuePoolWriter poolWriter = writableValuePool.getPoolWriter();
    final int valueOffset = poolWriter.position();

    final int calculatedSize = ValuePoolWriter.calculateValueSize(testType, testValue);
    final long valueDisplacement = poolWriter.writeValue(testType, testValue);
    assertThat(calculatedSize, is(poolWriter.position() - valueOffset));
    assertThat(valueDisplacement, is(expectedDisplacement));

    buffer.rewind();

    final ValuePool readableValuePool = new ValuePool(buffer, this.codec);
    final ValuePoolReader poolReader = readableValuePool.getPoolReader();
    final Object observedValue = poolReader.readValue(testType, valueDisplacement);

    assertThat(observedValue, is(testValue));
  }

  /**
   * Exercises {@link ValuePoolWriter} and {@link ValuePoolReader} for Sovereign primitive types as a key.
   * Primitive type values fit into the {@link CellDescriptor#valueDisplacement} field.
   *
   * @param testValue the test value to use
   * @param expectedDisplacement the expected primitive value encoding
   */
  private <K extends Comparable<K>> void testPrimitiveKey(final K testValue, final long expectedDisplacement)
      throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    final ValuePool writableValuePool =
        new ValuePool(buffer, buffer.capacity(), this.codec);
    final ValuePoolWriter poolWriter = writableValuePool.getPoolWriter();
    final int valueOffset = poolWriter.position();

    final int calculatedSize = ValuePoolWriter.calculateKeySize(testValue);
    final CellDescriptor keyDescriptor = poolWriter.writeKey(testValue);
    assertThat(calculatedSize, is(poolWriter.position() - valueOffset));
    assertThat(keyDescriptor.valueDisplacement, is(expectedDisplacement));

    buffer.rewind();

    final ValuePool readableValuePool = new ValuePool(buffer, this.codec);
    final ValuePoolReader poolReader = readableValuePool.getPoolReader();
    final Object observedValue = poolReader.readKey(keyDescriptor);

    assertThat(observedValue, is(testValue));
  }

  /**
   * Exercises {@link ValuePoolWriter} and {@link ValuePoolReader} for Sovereign non-primitive types.
   *
   * @param testType the Sovereign type to test
   * @param testValue the test value to use
   */
  private void testNonPrimitiveValue(final SovereignType testType, final Object testValue) throws IOException {
    final byte[] fence = { (byte)0xEB, (byte)0xAB, (byte)0xEF, (byte)0xAC };
    final ByteBuffer buffer = ByteBuffer.allocate(130 * 4096);
    final ValuePool writableValuePool =
        new ValuePool(buffer, buffer.capacity(), this.codec);
    final ValuePoolWriter poolWriter = writableValuePool.getPoolWriter();
    final int valueOffset = poolWriter.position();

    final int calculatedSize = ValuePoolWriter.calculateValueSize(testType, testValue);
    final long firstValueDisplacement = poolWriter.writeValue(testType, testValue);
    assertThat(calculatedSize, is(poolWriter.position() - valueOffset));

    final long fenceDisplacement = poolWriter.writeValue(SovereignType.BYTES, fence);
    final long secondValueDisplacement = poolWriter.writeValue(testType, testValue);

    buffer.rewind();

    final ValuePool readableValuePool = new ValuePool(buffer, this.codec);
    final ValuePoolReader poolReader = readableValuePool.getPoolReader();

    assertThat(poolReader.readValue(testType, firstValueDisplacement), is(equalTo(testValue)));
    assertThat(poolReader.readValue(SovereignType.BYTES, fenceDisplacement), is(equalTo(fence)));
    assertThat(poolReader.readValue(testType, secondValueDisplacement), is(equalTo(testValue)));
  }

  /**
   * Exercises {@link ValuePoolWriter} and {@link ValuePoolReader} for Sovereign non-primitive types as a key.
   *
   * @param testValue the test value to use
   */
  private <K extends Comparable<K>> void testNonPrimitiveKey(final K testValue) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(130 * 4096);
    final ValuePool writableValuePool =
        new ValuePool(buffer, buffer.capacity(), this.codec);
    final ValuePoolWriter poolWriter = writableValuePool.getPoolWriter();
    final int valueOffset = poolWriter.position();

    final int calculatedSize = ValuePoolWriter.calculateKeySize(testValue);
    final CellDescriptor keyDescriptor = poolWriter.writeKey(testValue);
    assertThat(calculatedSize, is(poolWriter.position() - valueOffset));

    buffer.rewind();

    final ValuePool readableValuePool = new ValuePool(buffer, this.codec);
    final ValuePoolReader poolReader = readableValuePool.getPoolReader();
    final Object observedValue = poolReader.readKey(keyDescriptor);

    assertThat(observedValue, is(testValue));
  }

  /**
   * Exercises the {@code Cell} access methods of {@link ValuePoolWriter} and {@link ValuePoolReader}.
   *
   * @param cell the test {@code Cell}
   * @param <V> the cell value type
   */
  private <V> void testCell(final Cell<V> cell) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(65 * 4096);
    final ValuePool writableValuePool =
        new ValuePool(buffer, buffer.capacity(), this.codec);
    final ValuePoolWriter poolWriter = writableValuePool.getPoolWriter();
    final int valueOffset = poolWriter.position();

    final int calculatedSize =
        ValuePoolWriter.calculateValueSize(Utility.getType(cell), cell.value())
        + ValuePoolWriter.calculateNameSize(cell.definition().name());
    final CellDescriptor cellDescriptor = poolWriter.writeCell(cell);
    assertThat(calculatedSize, is(poolWriter.position() - valueOffset));

    buffer.rewind();

    final ValuePool readableValuePool = new ValuePool(buffer, this.codec);
    final ValuePoolReader poolReader = readableValuePool.getPoolReader();
    final Cell<?> observedCell = poolReader.readCell(cellDescriptor);

    assertThat(observedCell, is(cell));
  }
}
