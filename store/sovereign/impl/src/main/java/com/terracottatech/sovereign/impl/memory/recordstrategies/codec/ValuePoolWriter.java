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
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.store.Cell;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Manages writing to the value pool of a serialized {@code Record}.
 *
 * <table>
 *   <tr><th>Displacement</th><th>Size</th><th>Description</th></tr>
 *   <tr><td>{@code 0x0000}</td><td>4 bytes</td><td>Size (<i>N</i>), in bytes, of Value Pool Data</td></tr>
 *   <tr><td>{@code 0x0004}</td><td>4 bytes</td><td>Reserved</td></tr>
 *   <tr><td>{@code 0x0008}</td><td><i>N</i> bytes</td><td>Value Pool Data</td></tr>
 * </table>
 *
 * @see ValuePool
 * @see ValuePoolReader
 */
class ValuePoolWriter {

  /**
   * A reference to the data portion of the {@code ByteBuffer} containing the value pool.
   * This buffer initially has its position set to zero (the beginning of the value pool
   * data) and its limit set to the size of the value pool data area.
   */
  private final ByteBuffer valuePool;

  /**
   * The {@code TimeReferenceCodec} instance used to encode {@link TimeReference} values.
   */
  private final TimeReferenceCodec timeReferenceCodec;

  /**
   * The map of cell names to {@code ValuePool} offset to the name.
   */
  private final HashMap<String, Integer> nameToDisplacementMap;

  /**
   * Creates a new {@code ValuePoolWriter} mapped onto the {@code ByteBuffer} provided.
   *
   * @param valuePool the {@code ByteBuffer} holding the {@link ValuePool} data area; the
   *                  current position of the buffer must be the beginning of the value pool
   *                  data area and the limit must be the size of the data area
   * @param timeReferenceCodec the {@code TimeReferenceCodec} to use for encoding {@link TimeReference} values
   */
  ValuePoolWriter(final ByteBuffer valuePool, final TimeReferenceCodec timeReferenceCodec) {
    this.valuePool = valuePool.slice();
    this.timeReferenceCodec = timeReferenceCodec;
    this.nameToDisplacementMap = new HashMap<>();
  }

  /**
   * Gets the current position in the data area of the {@code ValuePool} buffer.
   *
   * @return the current position in the {@code ValuePool} buffer.
   */
  int position() {
    return this.valuePool.position();
  }

  /**
   * Appends a record key to this {@code ValuePoolWriter}.
   *
   * @param key the key value to append
   *
   * @return a new, unnamed {@code CellDescriptor} describing the just written key
   *
   * @throws IOException if an error is raised while writing to this {@code ValuePoolWriter}
   * @throws BufferOverflowException if the buffer is too small to hold the value
   * @throws NullPointerException if {@code key} is {@code null}
   */
  final CellDescriptor writeKey(final Object key) throws IOException, BufferOverflowException {
    Objects.requireNonNull(key, "key");

    final SovereignType keyType = Utility.getType(key.getClass());
    long offset = this.writeValue(keyType, key);
    return new CellDescriptor(0, keyType, 0, offset);
  }

  /**
   * Appends the serialized {@link TimeReference} to this {@code ValuePoolWriter} and returns a
   * {@link CellDescriptor} describing the value.  If the serialized value is 8 bytes or less,
   * the value is encoded in the returned {@code CellDescriptor}.
   *
   * @param timeReference the {@code TimeReference} instance to encode
   *
   * @return a new {@code CellDescriptor} describing the encoded value
   *
   * @throws IOException if thrown by the {@code ValuePoolWriter} implementation
   * @throws BufferOverflowException if the buffer is too small to hold the value
   */
  final CellDescriptor writeTimeReference(final TimeReference<?> timeReference) throws IOException,
    BufferOverflowException {
    return this.timeReferenceCodec.encode(this.valuePool, timeReference);
  }

  /**
   * Appends the name and value of a {@code Cell} to this {@code ValuePoolWriter}.
   *
   * @param cell the {@code Cell} to append
   *
   * @return a new {@code CellDescriptor} describing the just written {@code Cell}
   *
   * @throws IOException if an error is raised while writing to this {@code ValuePoolWriter}
   * @throws BufferOverflowException if the buffer is too small to hold the value
   */
  final CellDescriptor writeCell(final Cell<?> cell) throws IOException, BufferOverflowException {
    int nameDisplacement = this.writeName(cell.definition().name());
    final long valueDisplacement = this.writeCellValue(cell);
    return new CellDescriptor(0, cell.definition().type(), nameDisplacement, valueDisplacement);
  }

  /**
   * Appends a name to this {@code ValuePoolWriter}.  This method tracks names written to the
   * {@code ValuePool} underlying this writer and, for names already written to the pool, simply
   * returns the existing name displacement.
   *
   * @param name the name to append
   *
   * @return the offset into this {@code ValuePoolWriter} of the first byte of {@code name}
   *
   * @throws IOException if an error is raised while writing to this {@code ValuePoolWriter}
   * @throws BufferOverflowException if the buffer is too small to hold the value
   */
  private int writeName(final String name) throws IOException, BufferOverflowException {

    Integer nameDisplacement = this.nameToDisplacementMap.get(name);
    if (nameDisplacement == null) {
      nameDisplacement = this.valuePool.position();
      this.writeUTF(name);
      this.nameToDisplacementMap.put(name, nameDisplacement);
    }

    return nameDisplacement;
  }

  /**
   * Appends the value of a {@code Cell} to this {@code ValuePoolWriter}.  If the {@code Cell}
   * provided is a fixed length type fitting in 8 bytes, the value is not appended
   * to the value pool but is formatted according to the value type into a {@code long}
   * and that formatted value is returned.  If not a fixed length type, the {@code Cell}
   * value is appended to the value pool and the displacement into the pool of the
   * first byte of the value is returned.
   *
   * @param cell the {@code Cell} the value of which is to be added this {@code ValuePoolWriter}
   *
   * @return for variable-length types, a {@code long} containing the offset (in the high-order
   *      word) of the value into {@code valuePool} of the first byte of {@code cell.value()};
   *      for fixed-length types, a {@code long} containing the value shifted to the high-order
   *      bytes
   *
   * @throws IOException if an error is raised while writing to this {@code ValuePoolWriter}
   * @throws BufferOverflowException if the buffer is too small to hold the value
   */
  private long writeCellValue(final Cell<?> cell) throws IOException, BufferOverflowException {
    return this.writeValue(Utility.getType(cell), cell.value());
  }

  /**
   * Appends a value, of the designated type, to this {@code ValuePoolWriter}.  If the type is
   * a fixed-length type for which the value fits into 8 bytes, the value is not appended
   * to the value pool but is formatted according to the value type into a {@code long}
   * and that formatted value is returned.  If not a fixed-length type, the value is
   * appended to the value pool according to its type and the displacement into the
   * pool of the first byte of the value is returned.
   *
   * @param type the expected type of {@code value}
   * @param value the value to append to this {@code ValuePoolWriter}
   *
   * @return for variable-length types, a {@code long} containing the offset (in the high-order
   *      word) of the value into {@code valuePool} of the first byte of {@code cell.value()};
   *      for fixed-length types, a {@code long} containing the value shifted to the high-order
   *      bytes
   *
   * @throws IOException if an error is raised while writing to this {@code ValuePoolWriter}
   * @throws BufferOverflowException if the buffer is too small to hold the value
   * @throws ClassCastException if {@code value} is not of the declared type
   *
   * @see ValuePoolWriter#calculateValueSize(SovereignType, Object)
   * @see ValuePoolReader#readValue(SovereignType, long)
   */
  final long writeValue(final SovereignType type, final Object value) throws IOException, BufferOverflowException {
    long offset = (long)this.valuePool.position() << 32;               // 0xFFFFFFFF00000000
    switch (type) {
      case BOOLEAN:
        return (long)((Boolean)value ? 0x01 : 0x00) << 56;   // 0xFF00000000000000
      case BYTES:
        final byte[] bytesValue = (byte[])value;
        this.writeInt(bytesValue.length);
        this.write(bytesValue);
        return offset;
      case CHAR:
        return (long)((Character)value) << 48;               // 0xFFFF000000000000
      case DOUBLE:
        return Double.doubleToRawLongBits((Double)value);    // 0xFFFFFFFFFFFFFFFF
      case INT:
        return (long)((Integer)value) << 32;                 // 0xFFFFFFFF00000000
      case LONG:
        return (Long)value;                                  // 0xFFFFFFFFFFFFFFFF
      case STRING:
        this.writeUTF((String)value);
        return offset;
      default:
        throw new UnsupportedOperationException("SovereignType " + type + " unsupported");
    }
  }

  /**
   * Appends the {@code byte} array provided to this {@code ValuePoolWriter}.  The current position
   * is advanced by the size of the array.
   *
   * @param b the {@code byte} array to append
   *
   * @throws IOException if thrown by the {@code ValuePoolWriter} implementation
   * @throws BufferOverflowException if the buffer is too small to hold the value
   */
  private void write(final byte[] b) throws IOException, BufferOverflowException {
    try {
      this.valuePool.put(b);
    } catch (ReadOnlyBufferException e) {
      throw new IOException(e);
    }
  }

  /**
   * Appends an {@code int} to this {@code ValuePoolWriter}.  The current position is advanced by 4 bytes.
   *
   * @param v the {@code int} value to append
   *
   * @throws IOException if thrown by the {@code ValuePoolWriter} implementation
   * @throws BufferOverflowException if the buffer is too small to hold the value
   */
  private void writeInt(final int v) throws IOException, BufferOverflowException {
    try {
      this.valuePool.putInt(v);
    } catch (ReadOnlyBufferException e) {
      throw new IOException(e);
    }
  }

  /**
   * Appends the <i>modified</i> UTF-8 representation of the {@code String} provided to this
   * {@code ValuePoolWriter}.  The current position is advanced by the number of bytes required by
   * the modified UTF-8 representation (including a 2 byte length).
   * <p>
   * This implementation appends the modified UTF-8 encoded {@code String}, as described in
   * <a href="http://docs.oracle.com/javase/8/docs/api/java/io/DataInput.html#modified-utf-8">Modified UTF-8</a>,
   * to this value pool buffer.  This method <b>does not</b> perform the same sanity checks performed by
   * {@code DataOutput.writeUTF} -- the buffer must be adequately sized.
   *
   * @param str the {@code String} to encode
   *
   * @throws IOException if the buffer is too small for the encoded {@code str}
   * @throws BufferOverflowException if the buffer is too small to hold the value
   */
  private void writeUTF(final String str) throws IOException, BufferOverflowException {
    final ByteBuffer buffer = this.valuePool;
    try {
      StringTool.putUTF(buffer, str);
    } catch (ReadOnlyBufferException e) {
      throw new IOException(e);
    }
  }

  /**
   * Calculates the number of bytes required in the value pool for the key provided.
   *
   * @param key the key
   * @param <K> the key type
   *
   * @return the number of bytes required to hold the key
   */
  static <K extends Comparable<K>> int calculateKeySize(final K key) {
    Objects.requireNonNull(key, "key");
    return calculateValueSize(Utility.getType(key.getClass()), key);
  }

  /**
   * Calculates the number of bytes required in the value pool to hold the data (exclusive of key) of the
   * record provided.
   *
   * @param record the {@code SovereignPersistentRecord} instance
   * @param timeReferenceCodec the {@code TimeReferenceCodec} to use for {@link TimeReference} calculations
   * @param <K> the key type
   *
   * @return the number of bytes required to hold the data from {@code record}
   */
  static <K extends Comparable<K>>
  int calculateRecordDataSize(final SovereignPersistentRecord<K> record, final TimeReferenceCodec timeReferenceCodec) {

    final TimeReferenceCodec.Sizing timeReferenceSizer = new TimeReferenceCodec.Sizing(timeReferenceCodec);
    final Set<String> uniqueCellNames = new HashSet<>();
    int size = 0;
    for (final SovereignPersistentRecord<K> version : record.elements()) {
      size += timeReferenceSizer.calculateSize(version.getTimeReference());
      for (final Cell<?> cell : version.cells().values()) {
        final String cellName = cell.definition().name();
        if (!uniqueCellNames.contains(cellName)) {
          uniqueCellNames.add(cellName);
          size += calculateNameSize(cellName);
        }
        size += calculateValueSize(Utility.getType(cell), cell.value());
      }
    }
    return size;
  }

  /**
   * Calculates the number of bytes required in the value pool for the value provided.
   *
   * @param type the type of {@code value}
   * @param value the value
   *
   * @return the number of bytes {@code value} would take if written to the value
   *      pool by {@link ValuePoolWriter#writeValue(SovereignType, Object)}
   */
  static int calculateValueSize(final SovereignType type, final Object value) {
    switch (type) {

      case STRING:
        return StringTool.getLengthAsUTF((String)value);

      case BYTES:
        return Integer.BYTES + ((byte[])value).length;

      case BOOLEAN:
      case CHAR:
      case DOUBLE:
      case INT:
      case LONG:
        return 0;                                           // No space used in value pool
      default:
        throw new UnsupportedOperationException("SovereignType " + type + " unsupported");
    }
  }

  /**
   * Calculates the number of bytes required in the value pool for the name provided.
   *
   * @param name the name
   *
   * @return the number of bytes {@code name} could take if written to the value pool
   *      by {@link #writeUTF(String)}
   */
  static int calculateNameSize(final String name) {
    return calculateValueSize(SovereignType.STRING, name);
  }
}
