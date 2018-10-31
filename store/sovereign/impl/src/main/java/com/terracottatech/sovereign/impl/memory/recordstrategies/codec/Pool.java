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

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * The base for a {@code ByteBuffer}-based data pool prefixed with a pool size.
 *
 * @author Clifford W. Johnson
 */
abstract class Pool {

  /**
   * The capacity, in bytes, of this {@code Pool}.  The capacity <i>excludes</i> any header for this pool.
   */
  private final int poolCapacity;

  /**
   * The pool data.  This {@code ByteBuffer} may be a slice of a larger buffer.  The initial position
   * of this buffer is beyond the header of this {@code Pool} instance and the limit is the real pool
   * size ({@link #poolCapacity} + {@link #getSerializedSize()}).
   */
  protected final ByteBuffer poolBuffer;

  /**
   * Creates a new {@code Pool} instance in the {@code ByteBuffer} provided.  The
   * position in {@code buffer} is advanced to the end of this {@code Pool} instance.
   * The buffer view retained for the pool is positioned beyond the header.
   *
   * @param buffer the {@code ByteBuffer} to contain this {@code Pool}
   * @param poolSize the size, including the header ({@link #getSerializedSize()}, for this pool
   *
   * @throws BufferOverflowException if {@code buffer} is too small to contain a pool of size {@code poolSize}
   * @throws IllegalArgumentException if pool capacity calculated from {@code poolSize} is negative
   */
  protected Pool(final ByteBuffer buffer, final int poolSize) throws BufferOverflowException {
    Objects.requireNonNull(buffer, "buffer");

    this.poolCapacity = poolSize - getSerializedSize();
    if (this.poolCapacity < 0) {
      throw new IllegalArgumentException("poolCapacity must be non-negative: " + this.poolCapacity);
    }

    buffer.putInt(this.poolCapacity);
    buffer.putInt(0);

    final ByteBuffer poolBuffer = buffer.slice();
    try {
      poolBuffer.limit(this.poolCapacity);
      buffer.position(buffer.position() + this.poolCapacity);
    } catch (IllegalArgumentException e) {
      final BufferOverflowException overflowException = new BufferOverflowException();
      overflowException.initCause(e);
      throw overflowException;
    }

    this.poolBuffer = poolBuffer;
  }

  /**
   * Creates a new {@code Pool} instance from the content in the {@code ByteBuffer} provided at its current
   * position.  The position in {@code buffer} is advanced to the end of this {@code Pool}.
   * The buffer view retained for the pool is positioned beyond the header.
   *
   * @param buffer the {@code ByteBuffer} from which this pool is taken
   *
   * @throws BufferUnderflowException if {@code buffer} is too small (truncated) to contain the
   *      pool at the current position
   */
  protected Pool(final ByteBuffer buffer) throws BufferUnderflowException {
    Objects.requireNonNull(buffer, "buffer");

    this.poolCapacity = buffer.getInt();
    buffer.getInt();                        // reserved bytes

    final ByteBuffer poolBuffer = buffer.slice().asReadOnlyBuffer();

    /*
     * Set the limit of the pool to the pool size and advance the input buffer beyond the pool.
     * Convert the IllegalArgumentException thrown from limit() or position() when the buffer is
     * too small for the argument to a BufferOverflowException
     */
    try {
      poolBuffer.limit(this.poolCapacity);
      buffer.position(buffer.position() + this.poolCapacity);
    } catch (IllegalArgumentException e) {
      final BufferUnderflowException underflowException = new BufferUnderflowException();
      underflowException.initCause(e);
      throw underflowException;
    }

    this.poolBuffer = poolBuffer;
  }

  /**
   * Returns the number of bytes required for the header of a {@code Pool} instance.
   *
   * @return the {@code Pool} header size
   */
  protected static int getSerializedSize() {
    return Integer.BYTES      // poolCapacity
        + Integer.BYTES;      // reserved
  }
}
