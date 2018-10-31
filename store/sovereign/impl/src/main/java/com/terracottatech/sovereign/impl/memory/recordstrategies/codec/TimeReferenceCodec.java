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
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages encoding and decoding {@link TimeReference} values to and from a {@code ByteBuffer}.
 *
 * @author Clifford W. Johnson
 */
final class TimeReferenceCodec {

  /**
   * The {@code TimeReferenceGenerator} instance used for encoding and decoding {@code TimeReference} values.
   */
  private final TimeReferenceGenerator<?> generator;

  /**
   * The maximum serialized length of a {@link TimeReference} value encoded by this {@code TimeReferenceCodec}.
   * This value does <b>not</b> include the length field preceding a value encoded into a target {@code ByteBuffer}.
   */
  private final AtomicInteger maxSerializedLength = new AtomicInteger();

  /**
   * Creates a new {@code TimeReferenceCodec} using the {@link TimeReferenceGenerator} provided.
   *
   * @param generator the {@code TimeReferenceGenerator} to use when processing {@link TimeReference} instances
   */
  TimeReferenceCodec(final TimeReferenceGenerator<?> generator) {
    this.generator = generator;
    this.maxSerializedLength.set(this.generator.maxSerializedLength());
  }

  /**
   * Gets the maximum encoded length for {@link TimeReference} instances processed by this
   * {@code TimeReferenceCodec}.
   *
   * @return the maximum length, excluding the length field, of encoded {@code TimeReference} values
   */
  public int getMaxSerializedLength() {
    return this.maxSerializedLength.get();
  }

  /**
   * Encodes the {@link TimeReference} provided into the supplied {@link ValuePool} buffer.  When encoding is
   * complete, the position of the {@code ByteBuffer} provided is the byte beyond the encoded value.
   *
   * @param buffer the {@link ValuePool} buffer positioned to the location at which {@code timeReference}
   *               is to be encoded
   * @param timeReference the {@code TimeReference} to encode
   *
   * @return a new {@code CellDescriptor} describing the encoded {@code TimeReference}
   *
   * @throws IOException if an error is raised from {@link TimeReferenceGenerator#put(ByteBuffer, TimeReference)}
   * @throws BufferOverflowException if {@code buffer} is too small to hold the encoded value
   */
  CellDescriptor encode(final ByteBuffer buffer, final TimeReference<?> timeReference)
      throws IOException, BufferOverflowException {
    int permittedValueLength = Math.min(this.maxSerializedLength.get(), buffer.remaining() - 4);

    ByteBuffer targetBuffer;
    int serializedLength = -1;
    boolean tooSmall;
    boolean lengthUpdated = false;
    do {
      tooSmall = false;
      targetBuffer = this.mapTargetBuffer(buffer, permittedValueLength);
      try {
        final int startPosition = targetBuffer.position();
        this.generator.put(targetBuffer, timeReference);
        serializedLength = targetBuffer.position() - startPosition;
      } catch (BufferOverflowException e) {
        if (permittedValueLength >= buffer.remaining() - 4) {
          throw e;
        }
        permittedValueLength = Math.min(buffer.remaining() - 4, permittedValueLength + 512);
        tooSmall = true;
        lengthUpdated = true;
      }
    } while (tooSmall);

    if (lengthUpdated) {
      this.maxSerializedLength.accumulateAndGet(serializedLength, Math::max);
    }

    /*
     * Create the CellDescriptor to return for the serialized TimeReference.
     */
    CellDescriptor descriptor;
    if (serializedLength > 8) {
      /*
       * If the serialized length is more than 8 bytes, the serialized TimeReference
       * was written in the value pool buffer and must be preceded by the serialized
       * length.  The buffer must be positioned *after* the serialized TimeReference.
       */
      long offset = (long)buffer.position() << 32;               // 0xFFFFFFFF00000000
      buffer.putInt(serializedLength);
      buffer.position(buffer.position() + serializedLength);
      descriptor = new CellDescriptor(0x0000, SovereignType.BYTES, 0, offset);

    } else {
      /*
       * When the serialized length is less that or equal to 8 bytes, the serialized
       * TimeReference can fit into the CellDescriptor `valueDisplacement` field.
       * The serialized value could be in the locally allocated, 8-byte buffer or
       * be in the caller-supplied buffer (offset by 4 bytes) -- handling is
       * slightly different for each.
       */
      if (targetBuffer.limit() == 8) {
        // Using locally allocated buffer, ensure we have a long available
        while (targetBuffer.hasRemaining()) {
          targetBuffer.put((byte)0x00);
        }
        targetBuffer.clear();
      } else {
        while (12 - targetBuffer.position() > 0) {
          targetBuffer.put((byte)0x00);
        }
        targetBuffer.clear().position(4);
      }
      descriptor = new CellDescriptor(0x0000, SovereignType.LONG, 0, targetBuffer.getLong());
    }

    return descriptor;
  }

  /**
   * Decodes a {@code TimeReference} instance, as described by the {@link CellDescriptor} provided, using
   * the {@link ValuePool} buffer provided.
   *
   * @param buffer the {@link ValuePool} buffer
   * @param timeReferenceCellDescriptor the {@code CellDescriptor} describing the a {@code TimeReference}
   *
   * @return a new {@code TimeReference} instance built from {@code buffer}
   *
   * @throws IOException if an error is raised from {@link TimeReferenceGenerator#get(ByteBuffer)}
   * @throws BufferUnderflowException if {@code buffer} is too small to hold the {@code TimeReference}
   *        instance as described by {@code timeReferenceCellDescriptor}
   */
  TimeReference<?> decode(final ByteBuffer buffer, final CellDescriptor timeReferenceCellDescriptor)
      throws IOException, BufferUnderflowException {

    final ByteBuffer view;
    final SovereignType valueType = timeReferenceCellDescriptor.valueType;
    if (valueType == SovereignType.LONG) {
      /*
       * The TimeReference value is encoded into the CellDescriptor.valueDisplacement field
       */
      view = ByteBuffer.allocate(Long.BYTES);
      view.putLong(timeReferenceCellDescriptor.valueDisplacement);
      view.flip();

    } else if (valueType == SovereignType.BYTES) {
      /*
       * The TimeReference value is encoded into the ValuePool.
       */
      view = buffer.duplicate();
      try {
        view.position((int)(timeReferenceCellDescriptor.valueDisplacement >>> 32));
        int serializedLength = view.getInt();
        view.limit(view.position() + serializedLength);
      } catch (IllegalArgumentException e) {
        final BufferUnderflowException underflowException = new BufferUnderflowException();
        underflowException.initCause(e);
        throw underflowException;
      }
    } else {
      throw new AssertionError("Unexpected TimeReference type: " + valueType);
    }

    try {
      return this.generator.get(view);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Provides a {@code ByteBuffer} into which a {@link TimeReference} is serialized.
   * If the expected length is 8 bytes or less, a newly allocated buffer is used because
   * a value of no more than 8 bytes is <b>not</b> stored in the value pool buffer.
   *
   * @param buffer the {@link ValuePool} buffer
   * @param permittedValueLength the anticipated serialized length
   *
   * @return a {@code ByteBuffer} with a limit of at least {@code permittedValueLength} bytes
   *
   * @throws BufferOverflowException if {@code buffer} is too small for the expected length
   */
  private ByteBuffer mapTargetBuffer(final ByteBuffer buffer, final int permittedValueLength)
      throws BufferOverflowException {
    final ByteBuffer targetBuffer;
    if (permittedValueLength <= 8) {
      targetBuffer = ByteBuffer.allocate(8);
    } else {
      targetBuffer = buffer.slice();
      try {
        targetBuffer.position(4);               // Save space for the `int` length field
        targetBuffer.limit(4 + permittedValueLength);
      } catch (IllegalArgumentException e) {
        final BufferOverflowException overflowException = new BufferOverflowException();
        overflowException.initCause(e);
        throw overflowException;
      }
    }
    return targetBuffer;
  }

  /**
   * Provides "sizing" services for {@link TimeReference} using a fixed {@link TimeReferenceCodec} instance.
   * This class uses a common {@code ByteBuffer} into which each {@code TimeReference} instance is encoded
   * to determine its size making the methods in this class <b>not</b> thread-safe.
   *
   */
  static final class Sizing {

    private ByteBuffer buffer;
    private final TimeReferenceCodec codec;

    /**
     * Creates a {@code Sizing} instance using the {@link TimeReferenceCodec} provided.
     *
     * @param codec the {@code TimeReferenceCodec} to use while sizing {@code TimeReference} instances
     */
    Sizing(final TimeReferenceCodec codec) {
      this.codec = codec;
      this.buffer = ByteBuffer.allocate(Math.max(512, this.codec.getMaxSerializedLength()));
    }

    /**
     * Calculates the {@link ValuePool} encoded size of the {@code TimeReference} provided by calling
     * {@link TimeReferenceCodec#encode(ByteBuffer, TimeReference)} using an internal buffer.
     * If {@code TimeReferenceCodec.encode} throws an {@code IOException}, the current
     * maximum encoded length is returned; the {@code IOException} will be observed again during
     * encoding.
     *
     * @param timeReference the {@code TimeReference} value to size
     *
     * @return the calculated {@code ValuePool} encoded size of {@code timeReference}; may be zero
     */
    int calculateSize(final TimeReference<?> timeReference) {
      this.buffer.clear();
      while (true) {
        try {
          this.codec.encode(this.buffer, timeReference);
          return this.buffer.position();
        } catch (BufferOverflowException e) {
          this.buffer = ByteBuffer.allocate(this.buffer.capacity() + 512);
        } catch (IOException e) {
          /*
           * If codec.getMaxSerializedLength is 8 or less, the ValuePool size is 0 --
           * all bytes are encoded into a CellDescriptor and the ValuePool is not impacted.
           * The codec.getMaxSerializedLength value does not include the preceding size
           * field so it gets added here.
           */
          final int maxSerializedLength = this.codec.getMaxSerializedLength();
          return (maxSerializedLength <= 8 ? 0 : maxSerializedLength + 4);
        }
      }
    }
  }
}
