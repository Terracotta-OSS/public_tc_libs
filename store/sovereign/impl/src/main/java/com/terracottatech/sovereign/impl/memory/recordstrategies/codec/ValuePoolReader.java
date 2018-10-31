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

import com.terracottatech.sovereign.common.utils.StringTool;
import com.terracottatech.sovereign.impl.SovereignType;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.store.Cell;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Manages reading from the {@link ValuePool} of a serialized {@code Record}.
 *
 * @see ValuePool
 * @see ValuePoolWriter
 */
final class ValuePoolReader {

  /**
   * A reference to the data portion of the {@code ByteBuffer} containing the value pool.
   * This buffer initially has its position set to zero (the beginning of the value pool
   * data) and its limit set to the size of the value pool data area.
   */
  private final ByteBuffer valuePool;

  /**
   * The {@code TimeReferenceCodec} instance to use for decoding {@link TimeReference} instances.
   */
  private final TimeReferenceCodec timeReferenceCodec;

  /**
   * Creates a new {@code ValuePoolReader} mapped onto the {@code ByteBuffer} provided.
   *
   * @param valuePool the {@code ByteBuffer} holding the {@link ValuePool} data area; the
   *                  current position of the buffer must be the beginning of the value pool
   *                  data area and the limit must be the size of the data area
   * @param timeReferenceCodec the {@code TimeReferenceCodec} to use for decoding {@link TimeReference} values
   */
  ValuePoolReader(final ByteBuffer valuePool, final TimeReferenceCodec timeReferenceCodec) {
    this.valuePool = valuePool.slice();
    this.timeReferenceCodec = timeReferenceCodec;
  }

  /**
   * Materializes the key from the {@link ValuePool} underlying this {@code ValuePoolReader}.
   *
   * @param keyDescriptor the {@code CellDescriptor} for the key
   * @param <K> the key type
   *
   * @return the extracted key
   *
   * @throws IOException if raised while trying to obtain the value from {@code valuePool}
   * @throws BufferUnderflowException if the buffer is too small to hold the declared value
   */
  @SuppressWarnings("unchecked")
  final <K extends Comparable<K>> K readKey(final CellDescriptor keyDescriptor)
      throws IOException, BufferUnderflowException {
    Objects.requireNonNull(keyDescriptor, "keyDescriptor");

    final SovereignType keyType = keyDescriptor.valueType;
    final Object key = this.readValue(keyType, keyDescriptor.valueDisplacement);
    return (K)keyType.getJDKType().cast(key);       // unchecked
  }

  /**
   * Gets a {@link TimeReference} value from the {@link ValuePool} underlying this {@code ValuePoolReader}.
   *
   * @param cellDescriptor the {@code CellDescriptor} for the encoded {@link TimeReference} value
   *
   * @return the {@code TimeReference} value
   *
   * @throws IOException if an error is raised while decoding the {@code TimeReference} value
   * @throws BufferUnderflowException if the buffer is too small to hold the encoded {@code TimeReference} value
   */
  final TimeReference<?> readTimeReference(final CellDescriptor cellDescriptor) throws IOException, BufferUnderflowException {
    Objects.requireNonNull(cellDescriptor, "cellDescriptor");
    // TODO: Do buffer position adjustments in this method
    return this.timeReferenceCodec.decode(this.valuePool, cellDescriptor);
  }

  /**
   * Materializes a {@link Cell} from the {@link ValuePool} underlying this {@code ValuePoolReader}.
   *
   * @param cellDescriptor the {@code CellDescriptor} for the encoded cell
   *
   * @return the extracted {@code Cell}
   *
   * @throws IOException if raised while trying to obtain the value from {@code valuePool}
   * @throws BufferUnderflowException if the buffer is too small to hold the declared value
   */
  final Cell<?> readCell(final CellDescriptor cellDescriptor) throws IOException, BufferUnderflowException {
    Objects.requireNonNull(cellDescriptor, "cellDescriptor");

    final String cellName = this.readName(cellDescriptor.nameDisplacement);
    final SovereignType valueType = cellDescriptor.valueType;
    final Object cellValue = this.readValue(valueType, cellDescriptor.valueDisplacement);
    return Utility.makeCell(cellDescriptor, cellName, cellValue);
  }

  /**
   * Extracts a value object from the {@link ValuePool} underlying this {@code ValuePoolReader}.  If the
   * type is a fixed-length type for which the value fits into 8 bytes, the value is actually encoded in
   * the displacement provided and not present in the value pool.
   *
   * @param type the type of the object to extract
   * @param displacement the displacement into {@code valuePool} of the object or the
   *                     encoded value
   * @return the value
   *
   * @throws IOException if raised while trying to obtain the value from {@code valuePool}
   * @throws BufferUnderflowException if the buffer is too small to hold the declared value
   *
   * @see ValuePoolWriter#writeValue(SovereignType, Object)
   */
  final Object readValue(final SovereignType type, final long displacement)
      throws IOException, BufferUnderflowException {
    switch (type) {
      case BOOLEAN:
        return (displacement >>> 56 != 0);
      case BYTES:
        this.setPosition((int)(displacement >>> 32));
        final int bytesSize = this.valuePool.getInt();
        final byte[] bytes = new byte[bytesSize];
        this.valuePool.get(bytes);
        return bytes;
      case CHAR:
        return (char)(displacement >>> 48);
      case DOUBLE:
        return Double.longBitsToDouble(displacement);
      case INT:
        return (int)(displacement >>> 32);
      case LONG:
        return displacement;
      case STRING:
        this.setPosition((int)(displacement >>> 32));
        return this.readUTF();
      default:
        throw new UnsupportedOperationException("SovereignType " + type + " unsupported");
    }
  }

  /**
   * Gets a name from the {@link ValuePool} underlying this {@code ValuePoolReader}.
   *
   * @param offset the displacement into this value pool of the name
   *
   * @return the name
   *
   * @throws IOException if raised while converting the name from UTF-8
   * @throws BufferUnderflowException if the buffer is too small to hold the declared value
   */
  private String readName(final int offset) throws IOException, BufferUnderflowException {
    this.setPosition(offset);
    return this.readUTF();
  }

  /**
   * Decodes a UTF-encoded string from this {@code ValuePoolReader}.
   * <p>
   * This implementation <b>does not</b> perform the validity checks performed in
   * {@link DataInputStream#readUTF()}.
   *
   * @return the decoded {@code String}
   *
   * @throws UTFDataFormatException if an error is detected while decoding the string
   * @throws BufferUnderflowException if the buffer is too small to hold the UTF encoded string
   */
  private String readUTF() throws UTFDataFormatException, BufferUnderflowException {
    return StringTool.getUTF(this.valuePool);
  }

  /**
   * Sets the position in {@link #valuePool} converting an {@code IllegalArgumentException} to a
   * {@code BufferUnderflowException}.
   *
   * @param offset the position to set
   *
   * @throws BufferUnderflowException if the buffer is too small for {@code offset}
   */
  private void setPosition(final int offset) throws BufferUnderflowException {
    try {
      this.valuePool.position(offset);
    } catch (IllegalArgumentException e) {
      final BufferUnderflowException underflowException = new BufferUnderflowException();
      underflowException.initCause(e);
      throw underflowException;
    }
  }
}
