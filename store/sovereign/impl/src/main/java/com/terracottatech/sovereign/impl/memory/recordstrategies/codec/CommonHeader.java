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

/**
 * Describes the common {@code Record} serialization header.
 *
 * @author Clifford W. Johnson
 */
@SuppressWarnings("unused")
abstract class CommonHeader {

  /**
   * The serialized record type indicator.  Processed as an unsigned {@code byte}.
   * The {@code 0b10000000} bit is reserved for identification of highly-compressed
   * record types.
   */
  protected final int recordType;

  /**
   * Indicates the serialized record is highly compressed.
   */
  public static final int HIGHLY_COMPRESSED_MASK = 0b10000000;

  /**
   * The format version for the serialized record. Processed as an unsigned {@code byte}.
   */
  protected final int serializationVersion;

  /**
   * Record flags.  Processed as an unsigned {@code short}.
   */
  protected final int flags;

  protected CommonHeader(final int recordType, final int serializationVersion, final int flags) {
    this.recordType = recordType;
    this.serializationVersion = serializationVersion;
    this.flags = flags;
  }

  /**
   * Creates a new {@code CommonHeader} instance from the serialized {@code CommonHeader} information from the
   * {@code ByteBuffer} instance provided.  This constructor uses <i>relative</i> {@code ByteBuffer} method and
   * affects the position of the buffer supplied.
   *
   * @param buffer the {@code ByteBuffer} from which the header is extracted
   *
   * @throws BufferUnderflowException if {@code buffer} is too small to hold this header
   */
  protected CommonHeader(final ByteBuffer buffer) throws BufferUnderflowException {
    this.recordType = Byte.toUnsignedInt(buffer.get());
    this.serializationVersion = Byte.toUnsignedInt(buffer.get());
    this.flags = Short.toUnsignedInt(buffer.getShort());
  }

  /**
   * Gets the serialization version encoded in this {@code CommonHeader}.
   *
   * @return the encoded serialization version
   */
  public int getSerializationVersion() {
    return serializationVersion;
  }

  /**
   * Gets the record type encoded in this {@code CommonHeader}.
   *
   * @return the record type
   */
  public int getRecordType() {
    return recordType;
  }

  /**
   * Appends the header data to the {@code ByteBuffer} instance provided.  This method uses
   * <i>relative</i> {@code ByteBuffer} methods and affects the position of the buffer supplied.
   *
   * @param buffer the {@code ByteBuffer} to which the header is appended.
   *
   * @throws BufferOverflowException if {@code buffer} is too small to hold this header
   */
  protected void put(final ByteBuffer buffer) throws BufferOverflowException {
    buffer.put((byte)this.recordType);
    buffer.put((byte)this.serializationVersion);
    buffer.putShort((short)this.flags);
  }

  /**
   * Gets the number of bytes required to serialize this portion of the header.
   *
   * @return the number of bytes required to serialize the {@code CommonHeader}
   */
  protected static int serializedSize() {
    return Byte.BYTES       // recordType
        + Byte.BYTES        // serializationVersion
        + Short.BYTES;      // flags
  }
}
