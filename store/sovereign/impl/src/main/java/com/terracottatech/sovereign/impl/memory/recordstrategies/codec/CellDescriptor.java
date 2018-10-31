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
import com.terracottatech.store.Type;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Class representing a deserialized cell descriptor.
 * <table>
 *   <tr><th>Displacement</th><th>Size</th><th>Description</th></tr>
 *   <tr><td>{@code 0x0000}</td><td>2 bytes</td><td>Flags</td></tr>
 *   <tr><td>{@code 0x0002}</td><td>2 bytes</td><td>Value Type ({@link SovereignType} ordinal, unsigned short)</td></tr>
 *   <tr><td>{@code 0x0004}</td><td>4 bytes</td><td>Name Displacement</td></tr>
 *   <tr><td>{@code 0x0008}</td><td>8 bytes</td><td>Value Displacement</td></tr>
 * </table>
 * The <i>Name Displacement</i> is the displacement into the value pool of the cell name value.
 * Depending on type, the <i>Value Displacement</i> is the left-aligned 4-byte displacement into the value pool
 * of the cell value <i>or</i> the left-aligned value.  The value content is managed by the user of the
 * instance of this class.
 */
final class CellDescriptor {
  final int cellFlags;                      // Only 2-bytes (unsigned) significant
  final SovereignType valueType;            // Only 2-bytes (unsigned) significant
  final int nameDisplacement;
  final long valueDisplacement;

  /**
   * Creates a new {@code CellDescriptor} using the {@link SovereignType}, flags, and displacements provided.
   *
   * @param cellFlags the flags to set for this {@code CellDescriptor}
   * @param valueType the {@code SovereignType} for this {@code CellDescriptor}
   * @param nameDisplacement the displacement into the {@link ValuePool} for the name of the cell described by
   *                         this {@code CellDescriptor}
   * @param valueDisplacement the displacement into the {@link ValuePool} for the value of the cell described by
   *                          this {@code CellDescriptor}
   *
   * @throws NullPointerException if {@code valueType} is {@code null}
   */
  CellDescriptor(final int cellFlags, final SovereignType valueType, final int nameDisplacement, final long valueDisplacement) {
    Objects.requireNonNull(valueType, "valueType");
    this.cellFlags = cellFlags;
    this.valueType = valueType;
    this.nameDisplacement = nameDisplacement;
    this.valueDisplacement = valueDisplacement;
  }

  /**
   * Creates a new {@code CellDescriptor} using the {@link Type}, flags, and displacements provided.
   *
   * @param cellFlags the flags to set for this {@code CellDescriptor}
   * @param valueType the {@code Type} for this {@code CellDescriptor}; this is converted to a {@link SovereignType}
   * @param nameDisplacement the displacement into the {@link ValuePool} for the name of the cell described by
   *                         this {@code CellDescriptor}
   * @param valueDisplacement the displacement into the {@link ValuePool} for the value of the cell described by
   *                          this {@code CellDescriptor}
   *
   * @throws EncodingException if {@code valueType} is not mappable to a {@link SovereignType}
   */
  CellDescriptor(final int cellFlags, final Type<?> valueType, final int nameDisplacement, final long valueDisplacement)
      throws EncodingException {
    this(cellFlags, Utility.getType(valueType), nameDisplacement, valueDisplacement);
  }

  /**
   * Creates a new {@code CellDescriptor} instance from the serialized {@code SingleRecord} information from the
   * {@code ByteBuffer} instance provided.  This constructor uses <i>relative</i> {@code ByteBuffer} method and
   * affects the position of the buffer supplied.
   *
   * @param buffer the {@code ByteBuffer} from which the cell descriptor is extracted
   *
   * @throws BufferUnderflowException if {@code buffer} is too small for a {@code CellDescriptor}
   */
  CellDescriptor(final ByteBuffer buffer) throws BufferUnderflowException {
    this.cellFlags = Short.toUnsignedInt(buffer.getShort());
    int valueTypeIndex = Short.toUnsignedInt(buffer.getShort());
    this.nameDisplacement = buffer.getInt();
    this.valueDisplacement = buffer.getLong();

    try {
      this.valueType = SovereignType.values()[valueTypeIndex];
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new EnumConstantNotPresentException(SovereignType.class, Integer.toString(valueTypeIndex));
    }
  }

  /**
   * Appends the cell descriptor data to the {@code ByteBuffer} instance provided.  This method uses
   * <i>relative</i> {@code ByteBuffer} methods and affects the position of the buffer supplied.
   *
   * @param buffer the {@code ByteBuffer} to which the cell descriptor is appended.
   *
   * @throws BufferOverflowException if {@code buffer} is too small for this {@code CellDescriptor}
   */
  void put(final ByteBuffer buffer) throws BufferOverflowException {
    buffer.putShort((short)this.cellFlags);
    buffer.putShort((short)this.valueType.ordinal());
    buffer.putInt(this.nameDisplacement);
    buffer.putLong(this.valueDisplacement);
  }

  /**
   * Gets the number of bytes required for a serialized {@code CellDescriptor} instance.
   *
   * @return the number of bytes required to serialize a {@code CellDescriptor} instance
   */
  static int serializedSize() {
    return Short.BYTES        // flags
        + Short.BYTES         // valueType
        + Integer.BYTES       // nameDisplacement
        + Long.BYTES;         // value / valueDisplacement
  }
}
