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

import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.testsupport.RecordGenerator;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReference;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;

/**
 * Tests operation of {@link VersionedRecordCodec}.
 *
 * @author Clifford W. Johnson
 */
public class VersionedRecordCodecTest {

  private static final int MAX_VERSION_COUNT = 15;

  private final SystemTimeReference.Generator generator = new SystemTimeReference.Generator();

  public VersionedRecordCodecTest() {
  }

  @Test
  public void testCtorBad() throws Exception {
    try {
      new VersionedRecordCodec<Long>((FixedTimeReference.Generator)null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    try {
      new VersionedRecordCodec<>((SovereignDataSetConfig<Long, FixedTimeReference>)null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testEncode1ArgSingle() throws Exception {
    final RecordGenerator<Long, SystemTimeReference> recordGenerator =
        new RecordGenerator<>(Long.class, this.generator);
    final VersionedRecordCodec<Long> codec = new VersionedRecordCodec<>(this.generator);

    final VersionedRecord<Long> originalRecord = recordGenerator.makeRecord(1);

    final VersionedRecordCodec.RecordSize recordSize = codec.calculateSerializedSize(originalRecord);

    final ByteBuffer buffer = codec.encode(originalRecord);
    assertThat(buffer, is(not(nullValue())));
    assertThat(buffer.capacity(), is(recordSize.getRecordSize()));
    assertThat(buffer.position(), is(0));

    assertThat(codec.getKey(buffer), is(equalTo(originalRecord.getKey())));
    assertThat(buffer.position(), is(0));

    final VersionedRecord<Long> decodedRecord = codec.decode(buffer);
    assertThat(decodedRecord, is(not(nullValue())));
    assertThat(decodedRecord, is(equalTo(originalRecord)));
    assertThat(decodedRecord.deepEquals(originalRecord), is(true));
  }

  @Test
  public void testEncode1ArgMultiple() throws Exception {
    final RecordGenerator<Long, SystemTimeReference> recordGenerator =
        new RecordGenerator<>(Long.class, this.generator);
    final VersionedRecordCodec<Long> codec = new VersionedRecordCodec<>(this.generator);

    final int recordCount = 15;
    for (int i = 0; i < recordCount; i++) {
      final VersionedRecord<Long> originalRecord = recordGenerator.makeRecord(MAX_VERSION_COUNT);

      final VersionedRecordCodec.RecordSize recordSize = codec.calculateSerializedSize(originalRecord);

      final ByteBuffer buffer = codec.encode(originalRecord);
      assertThat(buffer, is(not(nullValue())));
      assertThat(buffer.capacity(), is(recordSize.getRecordSize()));
      assertThat(buffer.position(), is(0));

      assertThat(codec.getKey(buffer), is(equalTo(originalRecord.getKey())));
      assertThat(buffer.position(), is(0));

      final VersionedRecord<Long> decodedRecord = codec.decode(buffer);
      assertThat(decodedRecord, is(not(nullValue())));
      assertThat(decodedRecord, is(equalTo(originalRecord)));
      assertThat(decodedRecord.deepEquals(originalRecord), is(true));
    }
  }

  @Test
  public void testEncode2Arg() throws Exception {
    final RecordGenerator<Long, SystemTimeReference> recordGenerator =
        new RecordGenerator<>(Long.class, this.generator);
    final VersionedRecordCodec<Long> codec = new VersionedRecordCodec<>(this.generator);

    final int recordCount = 15;

    final List<VersionedRecord<Long>> records = new ArrayList<>(recordCount);
    int bufferSize = 0;
    for (int i = 0; i < recordCount; i++) {
      final VersionedRecord<Long> record = recordGenerator.makeRecord(MAX_VERSION_COUNT);
      records.add(record);
      bufferSize += codec.calculateSerializedSize(record).getRecordSize();
    }

    final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

    int expectedBufferPosition = 0;
    for (final VersionedRecord<Long> record : records) {
      final ByteBuffer sameBuffer = codec.encode(buffer, record);

      assertThat(sameBuffer, is(sameInstance(buffer)));
      expectedBufferPosition += codec.calculateSerializedSize(record).getRecordSize();
      assertThat(buffer.position(), is(expectedBufferPosition));
    }

    buffer.rewind();

    expectedBufferPosition = 0;
    for (final VersionedRecord<Long> expectedRecord : records) {
      assertThat(codec.getKey(buffer), is(equalTo(expectedRecord.getKey())));
      assertThat(buffer.position(), is(expectedBufferPosition));

      final VersionedRecord<Long> decodedRecord = codec.decode(buffer);

      expectedBufferPosition += codec.calculateSerializedSize(expectedRecord).getRecordSize();
      assertThat(buffer.position(), is(expectedBufferPosition));

      assertThat(decodedRecord, is(equalTo(expectedRecord)));
      assertThat(decodedRecord.deepEquals(expectedRecord), is(true));
    }
  }
}
