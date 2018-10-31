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

import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import org.junit.Test;

import com.terracottatech.sovereign.impl.SovereignType;
import com.terracottatech.sovereign.impl.memory.SingleRecord;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.Cell;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * @author Clifford W. Johnson
 */
public class RecordDescriptorPoolTest {

  private static final FixedTimeReference.Generator fixedGenerator = new FixedTimeReference.Generator();

  private final Random rnd = new Random(8675309L);

  @Test
  public void testCtorNewBad() throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    try {
      new RecordDescriptorPool<String>(null, RecordDescriptorPool.getSerializedSize());
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    try {
      new RecordDescriptorPool<String>(buffer, RecordDescriptorPool.getSerializedSize() - 1);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new RecordDescriptorPool<String>(ByteBuffer.allocate(0), RecordDescriptorPool.getSerializedSize());
      fail();
    } catch (BufferOverflowException e) {
      // expected
    }
  }

  @Test
  public void testCtorExistingBad() throws Exception {
    try {
      new RecordDescriptorPool<String>(ByteBuffer.allocate(RecordDescriptorPool.getSerializedSize() - 1));
      fail();
    } catch (BufferUnderflowException e) {
      // expected
    }
  }

  @Test
  public void testSmallestLength() throws Exception {
    final SingleRecord<Long> minimalRecord =
        new SingleRecord<>(new VersionedRecord<>(), 1L, fixedGenerator.get(), 1L);

    final TimeReferenceCodec codec = new TimeReferenceCodec(fixedGenerator);
    final int descriptorPoolSize = RecordDescriptorPool.calculatePoolSize(minimalRecord);
    final int valuePoolSize = ValuePool.calculatePoolSize(minimalRecord, codec);

    System.out.format("descriptorPoolSize=%d; valuePoolSize=%d%n", descriptorPoolSize, valuePoolSize);

    final ByteBuffer buffer = ByteBuffer.allocate(descriptorPoolSize + valuePoolSize);

    final RecordDescriptorPool<Long> writerDescriptorPool =
        new RecordDescriptorPool<>(buffer, descriptorPoolSize);
    final ValuePool writableValuePool = new ValuePool(buffer, valuePoolSize, codec);
    final ValuePoolWriter poolWriter = writableValuePool.getPoolWriter();

    writerDescriptorPool.put(minimalRecord, poolWriter);

    buffer.rewind();

    final RecordDescriptorPool<Long> readerDescriptorPool = new RecordDescriptorPool<>(buffer);
    final ValuePool readerValuePool = new ValuePool(buffer, codec);
    final ValuePoolReader poolReader = readerValuePool.getPoolReader();

    final VersionedRecord<Long> parent = new VersionedRecord<>();
    final SingleRecord<Long> observedRecord = readerDescriptorPool.get(parent, 1L, poolReader);

    assertThat(observedRecord.deepEquals(minimalRecord), is(true));
  }

  @Test
  public void testMultiVersionRecord() throws Exception {
    final SerializableTestTimeReference.Generator generator = new SerializableTestTimeReference.Generator();

    final String key = this.getLargeKey();
    final VersionedRecord<String> originalParent = new VersionedRecord<>();

    long msn = Long.MIN_VALUE;
    final LinkedList<SingleRecord<String>> versions = new LinkedList<>();
    final Map<String, Cell<?>> cells = new LinkedHashMap<>();
    for (int i = 0; i < 20; i++) {

      cells.clear();
      for (final SovereignType type : SovereignType.values()) {
        final Cell<?> cell = this.makeCell(type.getJDKType());
        cells.put(cell.definition().name(), cell);
      }

      versions.addFirst(new SingleRecord<>(originalParent, key, generator.get(), ++msn, cells));
    }

    originalParent.elements().addAll(versions);

    final TimeReferenceCodec codec = new TimeReferenceCodec(generator);
    final int descriptorPoolSize = RecordDescriptorPool.calculatePoolSize(originalParent);
    final int valuePoolSize = ValuePool.calculatePoolSize(originalParent, codec);

    final ByteBuffer buffer = ByteBuffer.allocate(descriptorPoolSize + valuePoolSize);

    final RecordDescriptorPool<String> writableDescriptorPool =
        new RecordDescriptorPool<>(buffer, descriptorPoolSize);
    final ValuePool writableValuePool = new ValuePool(buffer, valuePoolSize, codec);
    final ValuePoolWriter poolWriter = writableValuePool.getPoolWriter();

    for (final SovereignPersistentRecord<String> version : originalParent.elements()) {
      writableDescriptorPool.put(version, poolWriter);
    }

    buffer.rewind();

    final RecordDescriptorPool<String> readableDescriptorPool =
        new RecordDescriptorPool<>(buffer);
    final ValuePool readableValuePool = new ValuePool(buffer, codec);
    final ValuePoolReader poolReader = readableValuePool.getPoolReader();

    final VersionedRecord<String> newParent = new VersionedRecord<>();
    // This test didn't include recording the version count
    for (int i = 0; i < originalParent.elements().size(); i++) {
      final SingleRecord<String> version =
          readableDescriptorPool.get(newParent, key, poolReader);
      newParent.addLast(version);
    }

    assertThat(newParent, is(equalTo(originalParent)));
    assertThat(originalParent.deepEquals(newParent), is(true));
  }

  /**
   * Generates a new {@code Cell} with pseudo-random content.  The cell name is the same as its type.
   *
   * @param type the cell value type
   * @param <V> the cell value type
   *
   * @return a new {@code Cell} with random content
   */
  @SuppressWarnings("unchecked")
  private <V> Cell<V> makeCell(final Class<V> type) {
    final SovereignType sovereignType = SovereignType.forJDKType(type);
    if (sovereignType == null) {
      throw new IllegalArgumentException();
    }

    final String name;
    final Object value;
    switch (sovereignType) {
      case BOOLEAN:
        name = "boolean";
        value = rnd.nextBoolean();
        break;
      case CHAR:
        name = "char";
        value = Character.toChars(rnd.nextInt(Character.MAX_CODE_POINT))[0];
        break;
      case STRING:
        name = "string";
        value = this.randomString();
        break;
      case INT:
        name = "int";
        value = rnd.nextInt();
        break;
      case LONG:
        name = "long";
        value = rnd.nextLong();
        break;
      case DOUBLE:
        name = "double";
        value = rnd.nextDouble();
        break;
      case BYTES: {
        name = "bytes";
        final byte[] bytes = new byte[rnd.nextInt(32768)];
        rnd.nextBytes(bytes);
        value = bytes;
        break;
      }
      default:
        throw new IllegalArgumentException();
    }

    return Cell.cell(name, (V)value);     // unchecked
  }

  /**
   * Generates a large, pseudo-random {@code String} for a key value.
   *
   * @return a pseudo-random string
   */
  private String getLargeKey() {
    final Random rnd = new Random(8675309L);
    final StringBuilder sb = new StringBuilder(65536 * 2);
    final int capacity = sb.capacity();
    while (sb.length() < capacity) {
      sb.appendCodePoint(rnd.nextInt(1 + 0x07FF));
    }
    return sb.toString();
  }

  /**
   * Generates and returns a pseudo-random {@code String} value.
   *
   * @return a random string
   */
  private String randomString() {
    final int length;
    if (rnd.nextBoolean()) {
      length = rnd.nextInt(1024);
    } else {
      length = rnd.nextInt(65536 * 2);
    }

    final StringBuilder sb = new StringBuilder(length);
    final int capacity = sb.capacity();
    while (sb.length() < capacity) {
      sb.appendCodePoint(rnd.nextInt(1 + 0x07FF));
    }
    return sb.toString();
  }
}
