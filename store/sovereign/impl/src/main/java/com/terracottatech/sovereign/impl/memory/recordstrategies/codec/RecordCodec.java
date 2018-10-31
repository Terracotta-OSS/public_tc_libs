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

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * Defines the serialization methods for {@link SovereignPersistentRecord} instances.
 *
 * @param <K> the key type for the {@code SovereignPersistentRecord}
 *
 * @author Clifford W. Johnson
 */
public interface RecordCodec<K extends Comparable<K>> {

  /**
   * Encodes a {@code SovereignPersistentRecord} instance in a portable byte sequence into a new {@code ByteBuffer}.
   * On return, the buffer's position is set to zero and it's limit is set to the size of the encoded record.
   *
   * @param record the {@code SovereignPersistentRecord} instance to encode
   *
   * @return a new {@code ByteBuffer} containing the serialized {@code record}
   *
   * @throws EncodingException if an error is encountered while serializing {@code record}
   */
  ByteBuffer encode(SovereignPersistentRecord<K> record) throws EncodingException;

  /**
   * Encodes a {@code SovereignPersistentRecord} instance into a portable byte sequence appending to an existing
   * {@code ByteBuffer}. The encoded record is appended at the current position of the buffer and, on return,
   * the position is beyond the added record.
   *
   * @param buffer the {@code ByteBuffer} to which the encoded record is added
   * @param record the {@code SovereignPersistentRecord} instance to encode
   *
   * @return {@code buffer} positioned beyond the added record
   *
   * @throws BufferOverflowException if {@code buffer} is too small to hold the encoded {@code record}
   * @throws EncodingException if an error is encountered while serializing {@code record}
   */
  ByteBuffer encode(ByteBuffer buffer, SovereignPersistentRecord<K> record) throws EncodingException,
    BufferOverflowException;

  /**
   * Constructs a {@code SovereignPersistentRecord} instance from the encoded data in the {@code ByteBuffer} provided.
   * On return, the buffer position is left at the end of the decoded record.
   *
   * @param buffer the {@code ByteBuffer} holding a serialized {@code SovereignPersistentRecord}; {@code buffer} must
   *               be positioned to the beginning of a serialized {@code SovereignPersistentRecord}
   *
   * @return a new {@code SovereignPersistentRecord}
   *
   * @throws EncodingException if an error is raised while deserializing the {@code SovereignPersistentRecord}
   * @throws BufferUnderflowException if {@code buffer} is too small (truncated) for the record described
   */
  SovereignPersistentRecord<K> decode(ByteBuffer buffer) throws EncodingException, BufferUnderflowException;

  /**
   * Extracts the key from {@code ByteBuffer} containing an encoded {@code SovereignPersistentRecord}.  On
   * return, the buffer is left positioned at the beginning of the encoded record.
   *
   * @param buffer the buffer, positioned at the beginning of an encoded record, from which the key is extracted
   *
   * @return the key from the encoded record
   *
   * @throws EncodingException if an error is raised while deserializing the {@code SovereignPersistentRecord}
   * @throws BufferUnderflowException if {@code buffer} is too small (truncated) for the record described
   */
  K getKey(ByteBuffer buffer) throws EncodingException, BufferUnderflowException;

  /**
   * Calculates the serialization size of the {@link SovereignPersistentRecord} provided.
   *
   * @param record the {@code SovereignPersistentRecord} to estimate
   *
   * @return a descriptor holding the calculated size values for the serialized version of {@code record}
   */
  RecordSize calculateSerializedSize(SovereignPersistentRecord<K> record);

  /**
   * A structure holding the values calculated by {@link RecordCodec#calculateSerializedSize(SovereignPersistentRecord)}.
   */
  class RecordSize {
    private final int headerSize;
    private final int valuePoolSize;

    public RecordSize(final int headerSize, final int valuePoolSize) {
      this.headerSize = headerSize;
      this.valuePoolSize = valuePoolSize;
    }

    public int getHeaderSize() {
      return this.headerSize;
    }

    public int getValuePoolSize() {
      return this.valuePoolSize;
    }

    public int getRecordSize() {
      return this.headerSize + this.valuePoolSize;
    }
  }
}
