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

import com.terracottatech.sovereign.impl.SovereignType;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.time.TimeReference;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Maps and provides accessors for the value pool in an encoded {@link SovereignPersistentRecord}.
 *
 * @author Clifford W. Johnson
 */
final class ValuePool extends Pool {

  /**
   * The {@code TimeReferenceCodec} to use when encoding and decoding {@link TimeReference} instances.
   */
  private final TimeReferenceCodec timeReferenceCodec;

  /**
   * The {@link ValuePoolReader} used read content from this {@code ValuePool}.
   * If {@link #poolWriter} is non-{@code null}, this field must be {@code null}.
   */
  private final ValuePoolReader poolReader;

  /**
   * The {@link ValuePoolWriter} used to add content to this {@code ValuePool}.
   * If {@link #poolReader} is non-{@code null}, this field must be {@code null}.
   */
  private final ValuePoolWriter poolWriter;

  /**
   * Maps a new, write-only {@code ValuePool} at the current position in the {@code ByteBuffer} provided.
   *
   * @param buffer the {@code ByteBuffer} in which the {@code ValuePool} is created
   * @param poolSize the size in bytes and including the header of the {@code ValuePool}
   * @param timeReferenceCodec the {@code TimeReferenceCodec} to use for encoding {@link TimeReference} in this pool
   *
   * @throws BufferOverflowException if {@code buffer} is too small for the pool
   * @throws IllegalArgumentException if pool capacity calculated from {@code poolSize} is negative
   */
  ValuePool(final ByteBuffer buffer, final int poolSize, final TimeReferenceCodec timeReferenceCodec)
      throws BufferOverflowException {
    super(buffer, poolSize);
    this.timeReferenceCodec = Objects.requireNonNull(timeReferenceCodec, "timeReferenceCodec");
    this.poolWriter = new ValuePoolWriter(this.poolBuffer, this.timeReferenceCodec);
    this.poolReader = null;
  }

  /**
   * Maps new, read-only {@code ValuePool} at the current position in the {@code ByteBuffer} provided.
   *
   * @param buffer the {@code ByteBuffer} containing the {@code ValuePool} to read
   * @param timeReferenceCodec the {@code TimeReferenceCodec} to use for decoding {@link TimeReference} in this pool
   *
   * @throws BufferUnderflowException if {@code buffer} is too small (truncated) for the declared size of the pool
   */
  ValuePool(final ByteBuffer buffer, final TimeReferenceCodec timeReferenceCodec) throws BufferUnderflowException {
    super(buffer);
    this.timeReferenceCodec = Objects.requireNonNull(timeReferenceCodec, "timeReferenceCodec");
    this.poolReader = new ValuePoolReader(this.poolBuffer, this.timeReferenceCodec);
    this.poolWriter = null;
  }

  /**
   * Estimates the size of the {@code ValuePool} required to serialize the {@link SovereignPersistentRecord}
   * provided.  This method ignores errors that are raised when calculating the size of a {@link TimeReference}
   * value.
   *
   * @param record the record used in the estimate
   * @param timeReferenceCodec the {@code TimeReferenceCodec} to use in encoding {@code TimeReference} instances
   *
   * @return the estimate of the size, including headers, of the {@code ValuePool} required to hold {@code record}
   *
   * @see TimeReferenceCodec.Sizing
   *
   * @throws EncodingException if {@code record.getKey} is not a valid {@link SovereignType}
   */
  static <K extends Comparable<K>>
  int calculatePoolSize(final SovereignPersistentRecord<K> record, final TimeReferenceCodec timeReferenceCodec)
      throws EncodingException {
    int size = Pool.getSerializedSize();        // pool header

    size += ValuePoolWriter.calculateKeySize(record.getKey());
    size += ValuePoolWriter.calculateRecordDataSize(record, timeReferenceCodec);

    return size;
  }

  /**
   * Gets the {@link ValuePoolReader} instance for this {@code ValuePool}.
   *
   * @return the {@code ValuePoolReader} instance for this pool
   *
   * @throws IllegalStateException if this pool was constructed for writing
   */
  ValuePoolReader getPoolReader() {
    if (this.poolReader == null) {
      throw new IllegalStateException("Attempting to obtain reader for write-only ValuePool");
    }
    return this.poolReader;
  }

  /**
   * Gets the {@link ValuePoolWriter} instance for this {@code ValuePool}.
   *
   * @return the {@code ValuePoolWriter} instance for this pool
   *
   * @throws IllegalStateException if this pool was constructed for reading
   */
  ValuePoolWriter getPoolWriter() {
    if (this.poolWriter == null) {
      throw new IllegalStateException("Attempting to obtain writer for read-only ValuePool");
    }
    return this.poolWriter;
  }
}
