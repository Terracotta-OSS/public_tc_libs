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

import com.terracottatech.sovereign.impl.memory.SingleRecord;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.store.Cell;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps and provides accessors for record metadata in an encoded {@link SovereignPersistentRecord}.
 *
 * @author Clifford W. Johnson
 */
final class RecordDescriptorPool<K extends Comparable<K>> extends Pool {

  /**
   * Creates a new {@code RecordDescriptorPool} instance in the {@code ByteBuffer} provided.  The position
   * in {@code buffer} is advanced to the end of this {@code RecordDescriptorPool} instance.
   *
   * @param buffer the {@code ByteBuffer} to contain this {@code RecordDescriptorPool}
   * @param poolSize the size, including the header, for this pool; the size should be calculated using
   *                 {@link #calculatePoolSize(SovereignPersistentRecord)}
   *
   * @throws BufferOverflowException if {@code buffer} is too small to contain a pool of {@code poolSize}
   * @throws IllegalArgumentException if pool capacity calculated from {@code poolSize} is negative
   */
  RecordDescriptorPool(final ByteBuffer buffer, final int poolSize) throws BufferOverflowException {
    super(buffer, poolSize);
  }

  /**
   * Creates a new {@code RecordDescriptorPool} instance from the content in the {@code ByteBuffer}
   * provided at its current position.  The position of {@code buffer} is advanced to the end of this
   * {@code RecordDescriptorPool}.
   *
   * @param buffer the {@code ByteBuffer} from which this pool is taken
   *
   * @throws BufferUnderflowException if {@code buffer} is too small (truncated) to hold the {@code RecordDescriptorPool}
   */
  RecordDescriptorPool(final ByteBuffer buffer) throws BufferUnderflowException {
    super(buffer);
  }

  /**
   * Calculates the number of bytes required to hold a {@code RecordDescriptorPool} for the
   * {@link SovereignPersistentRecord} provided.  Encoding a {@code SovereignPersistentRecord}
   * requires a record descriptor slot for each record version; the size of the record descriptor
   * varies with the number of cells in each version.
   *
   * @param record the {@code SovereignPersistentRecord} for which the calculation is made
   *
   * @return the size, including headers, required of a {@code RecordDescriptorPool} for {@code record}
   */
  static int calculatePoolSize(final SovereignPersistentRecord<?> record) {
    int size = Pool.getSerializedSize();    // pool header

    for (final SovereignPersistentRecord<?> version : record.elements()) {
      size += calculateVersionSize(version);
    }

    return size;
  }

  /**
   * Calculates the number of bytes required in the {@code RecordDescriptorPool} for the
   * {@code SovereignPersistentRecord} provided.
   *
   * @param version the {@code SovereignPersistentRecord} to use in the calculation
   *
   * @return the number of {@code RecordDescriptorPool} bytes required for {@code version}
   */
  private static int calculateVersionSize(final SovereignPersistentRecord<?> version) {
    int size = Integer.BYTES                  // cell count
        + Long.BYTES                          // MSN
        + CellDescriptor.serializedSize();    // TimeReference cell descriptor
    size += CellDescriptor.serializedSize() * version.cells().size();      // Per-cell cell descriptor
    return size;
  }

  /**
   * Records an encoding of the {@link SingleRecord} provided into this {@code RecordDescriptorPool}
   * and the {@link ValuePool} through the {@link ValuePoolWriter} provided.
   *
   * @param record the {@code SingleRecord} to encode and persist
   * @param valueWriter the {@code ValuePoolWriter} through which the {@code ValuePool} is written
   *
   * @throws IOException if an error is raised while encoding {@code record} or writing to {@code valueWriter}
   * @throws BufferOverflowException if the buffer is too small to hold {@code record}
   */
  void put(final SovereignPersistentRecord<K> record, final ValuePoolWriter valueWriter)
      throws IOException, BufferOverflowException {

    this.poolBuffer.putInt(record.cells().size());
    this.poolBuffer.putLong(record.getMSN());

    valueWriter.writeTimeReference(record.getTimeReference()).put(this.poolBuffer);

    for (final Cell<?> cell : record.cells().values()) {
      valueWriter.writeCell(cell).put(this.poolBuffer);
    }
  }

  /**
   * Reforms a {@link VersionedRecord} instance from the content of the current position of the
   * {@code RecordDescriptorPool} and the {@link ValuePool} through the {@link ValuePoolReader} provided.
   *
   * @param parent the {@code VersionedRecord} parent for the decoded {@code SingleRecord}
   * @param key the key for the new record
   * @param poolReader the {@code ValuePoolReader} used to access the {@code ValuePool}
   *
   * @return a new {@code SingleRecord} instance
   *
   * @throws IOException if an error is raised while decoding the record or reading from {@code poolReader}
   * @throws BufferUnderflowException if the buffer is too small (truncated) to hold the defined record
   */
  SingleRecord<K> get(final VersionedRecord<K> parent, final K key, final ValuePoolReader poolReader)
      throws IOException, BufferUnderflowException {

    final int cellCount = this.poolBuffer.getInt();
    final long msn = this.poolBuffer.getLong();

    final TimeReference<?> timeReference = poolReader.readTimeReference(new CellDescriptor(this.poolBuffer));

    final Map<String, Cell<?>> cells = new LinkedHashMap<>();
    for (int i = 0; i < cellCount; i++) {
      final Cell<?> cell = poolReader.readCell(new CellDescriptor(this.poolBuffer));
      cells.put(cell.definition().name(), cell);
    }

    return new SingleRecord<>(parent, key, timeReference, msn, cells);
  }
}
