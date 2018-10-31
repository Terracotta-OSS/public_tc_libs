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

import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.memory.RecordBufferStrategy;
import com.terracottatech.sovereign.impl.memory.SingleRecord;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author Clifford W. Johnson
 */
public final class VersionedRecordCodec<K extends Comparable<K>>
  implements RecordCodec<K>, RecordBufferStrategy<K> {

  /**
   * The serialization record type for {@link VersionedRecord}.
   */
  public static final int RECORD_TYPE = 0x01;

  /**
   * The current serialization version use by {@code VersionedRecord}.
   */
  public static final int SERIALIZATION_VERSION = 0x00;

  /**
   * The {@code TimeReferenceCodec} to use for managing encoded {@link TimeReference} values.
   */
  private final TimeReferenceCodec timeReferenceCodec;


  public VersionedRecordCodec(final TimeReferenceGenerator<?> generator) {
    Objects.requireNonNull(generator, "generator");
    this.timeReferenceCodec = new TimeReferenceCodec(generator);
  }

  public VersionedRecordCodec(final SovereignDataSetConfig<K, ?> config) {
    this(Objects.requireNonNull(config, "config").getTimeReferenceGenerator());
  }

  @Override
  public ByteBuffer toByteBuffer(final SovereignPersistentRecord<K> record) {
    return this.encode(record);
  }

  @Override
  public VersionedRecord<K> fromByteBuffer(final ByteBuffer buf) {
    return this.decode(buf.slice());
  }

  @Override
  public K readKey(final ByteBuffer buf) {
    return this.getKey(buf);
  }

  @Override
  public ByteBuffer encode(final SovereignPersistentRecord<K> record) throws EncodingException {
    Objects.requireNonNull(record, "record");

    final RecordSize recordSize = calculateSerializedSize(record);
    final ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize.getRecordSize());
    try {
      this.encodeInternal(recordBuffer, record, recordSize);
    } catch (BufferOverflowException e) {
      throw new AssertionError(e);        // Should not get this with calculated buffer size
    } catch (IOException e) {
      throw new EncodingException(e);
    }
    recordBuffer.clear();                 // Position buffer to beginning of serialized record
    return recordBuffer;
  }

  @Override
  public ByteBuffer encode(final ByteBuffer buffer, final SovereignPersistentRecord<K> record)
      throws EncodingException, BufferOverflowException {
    Objects.requireNonNull(buffer, "buffer");
    Objects.requireNonNull(record, "record");

    try {
      this.encodeInternal(buffer, record, calculateSerializedSize(record));
    } catch (IOException e) {
      throw new EncodingException(e);
    }
    return buffer;
  }

  @Override
  public VersionedRecord<K> decode(final ByteBuffer buffer) throws EncodingException, BufferUnderflowException {
    Objects.requireNonNull(buffer, "buffer");

    VersionedRecord<K> record = new VersionedRecord<>();
    try {
      final Header<K> header = new Header<>(buffer);
      final RecordDescriptorPool<K> recordDescriptorPool = new RecordDescriptorPool<>(buffer);
      final ValuePool valuePool = new ValuePool(buffer, this.timeReferenceCodec);
      final ValuePoolReader poolReader = valuePool.getPoolReader();

      final K key = header.getKey(poolReader);
      for (int i = 0; i < header.getVersionCount(); i++) {
        final SingleRecord<K> version = recordDescriptorPool.get(record, key, poolReader);
        record.addLast(version);
      }
    } catch (IOException e) {
      throw new EncodingException(e);
    }

    return record;
  }

  @Override
  public K getKey(final ByteBuffer buffer) throws EncodingException, BufferUnderflowException {
    Objects.requireNonNull(buffer, "buffer");

    final ByteBuffer view = buffer.duplicate();
    try {
      final Header<K> header = new Header<>(view);
      new RecordDescriptorPool<>(view);     // Required for positioning
      final ValuePool valuePool = new ValuePool(view, this.timeReferenceCodec);
      final ValuePoolReader poolReader = valuePool.getPoolReader();

      return header.getKey(poolReader);
    } catch (IOException e) {
      throw new EncodingException(e);
    }
  }

  /**
   * Calculates the serialization size of the {@link VersionedRecord} provided.
   *
   * @param record the {@code VersionedRecord} to estimate
   *
   * @return a descriptor holding the calculated size values for the serialized version of {@code record}
   *
   * @throws EncodingException if an error occurs while calculating the record size
   */
  @Override
  public RecordSize calculateSerializedSize(final SovereignPersistentRecord<K> record) throws EncodingException {
    return new RecordSize(
        Header.serializedSize(),
        RecordDescriptorPool.calculatePoolSize(record),
        ValuePool.calculatePoolSize(record, this.timeReferenceCodec));
  }

  /**
   * Encodes a record into the {@code ByteBuffer} provided.
   *
   * @param recordBuffer the {@code ByteBuffer} into which {@code record} is encoded
   * @param record the {@code VersionedRecord} to encode
   * @param sizeInfo the {@code RecordSize} instance returned from a call to
   *          {@link #calculateSerializedSize} for {@code record}
   *
   * @throws IOException if an error is raised while encoding {@code record}
   * @throws BufferOverflowException if the buffer is too small to hold {@code record}
   */
  private void encodeInternal(final ByteBuffer recordBuffer, final SovereignPersistentRecord<K> record, final RecordSize sizeInfo)
      throws IOException, BufferOverflowException {

    final int recordDescriptorPoolOffset = sizeInfo.getHeaderSize();
    final int valuePoolOffset = recordDescriptorPoolOffset + sizeInfo.getRecordDescriptorPoolSize();

    /*
     * Carve out buffers for the RecordDescriptorPool and ValuePool.
     * An IllegalArgumentException thrown by the ByteBuffer position
     * and limit methods are converted to a BufferOverflowException.
     */
    final ByteBuffer recordDescriptorBuffer;
    final ByteBuffer valuePoolBuffer;
    recordBuffer.mark();
    try {
      final int recordPosition = recordBuffer.position();
      recordBuffer.position(recordPosition + recordDescriptorPoolOffset);
      recordDescriptorBuffer = recordBuffer.slice();
      recordDescriptorBuffer.limit(sizeInfo.getRecordDescriptorPoolSize());

      recordBuffer.position(recordPosition + valuePoolOffset);
      valuePoolBuffer = recordBuffer.slice();
      valuePoolBuffer.limit(sizeInfo.getValuePoolSize());

    } catch (IllegalArgumentException e) {
      final BufferOverflowException overflowException = new BufferOverflowException();
      overflowException.initCause(e);
      throw overflowException;
    } finally {
      recordBuffer.reset();
    }

    final RecordDescriptorPool<K> recordDescriptorPool =
        new RecordDescriptorPool<>(recordDescriptorBuffer, sizeInfo.getRecordDescriptorPoolSize());
    final ValuePool valuePool =
        new ValuePool(valuePoolBuffer, sizeInfo.getValuePoolSize(), this.timeReferenceCodec);
    final ValuePoolWriter poolWriter = valuePool.getPoolWriter();

    /*
     * Write out the record header including the key and advance the position to the end of the record space.
     */
    final Header<K> header = new Header<>(0, record.elements().size(), record.getKey());
    header.put(recordBuffer, poolWriter);

    try {
      recordBuffer.position((recordBuffer.position() - sizeInfo.getHeaderSize()) + sizeInfo.getRecordSize());
    } catch (IllegalArgumentException e) {
      // This should have been caught above
      final BufferOverflowException overflowException = new BufferOverflowException();
      overflowException.initCause(e);
      throw overflowException;
    }

    /*
     * Now write out the record versions.
     */
    for (SovereignPersistentRecord<K> version : record.elements()) {
      recordDescriptorPool.put(version, poolWriter);
    }
  }

  /**
   * Describes the header for a serialized {@code VersionedRecord}.
   * <table>
   *   <tr><th>Displacement</th><th>Size</th><th>Description</th></tr>
   *   <tr><td>{@code 0x0000}</td><td>4 bytes</td><td>Common Header</tr>
   *   <tr><td>{@code 0x0004}</td><td>4 bytes</td><td>Version count</td></tr>
   *   <tr><td>{@code 0x0008}</td><td>16 bytes</td><td>Cell Descriptor for key</td></tr>
   * </table>
   */
  private static final class Header<K extends Comparable<K>> extends CommonHeader {

    private final int versionCount;
    private CellDescriptor keyDescriptor;
    private K key;

    Header(final int flags, final int versionCount, final K key) {
      super(RECORD_TYPE, SERIALIZATION_VERSION, flags);
      this.versionCount = versionCount;
      this.key = key;
    }

    /**
     * Creates a new {@code Header} instance from the serialized {@code SingleRecord} header information from the
     * {@code ByteBuffer} instance provided.  This constructor uses <i>relative</i> {@code ByteBuffer} methods and
     * affects the position of the buffer supplied.
     *
     * @param buffer the {@code ByteBuffer} from which the header is extracted
     *
     * @throws BufferUnderflowException if {@code buffer} is too small to hold this header
     */
    Header(final ByteBuffer buffer) throws BufferUnderflowException {
      super(buffer);

      if (this.recordType != RECORD_TYPE || this.serializationVersion != SERIALIZATION_VERSION) {
        throw new EncodingException(
            String.format(
                "Expecting recordType=0x%02X and serializationVersion=0x%02X; found recordType=0x%02X and serializationVersion=0x%2X",
                RECORD_TYPE, 0,
                this.recordType, this.serializationVersion));
      }

      this.versionCount = buffer.getInt();
      this.keyDescriptor = new CellDescriptor(buffer);
    }

    /**
     * Gets the number of bytes required for a serialized {@code Header} instance.
     *
     * @return the number of bytes required to serialize a {@code Header} instance
     */
    protected static int serializedSize() {
      return CommonHeader.serializedSize()
          + Integer.BYTES                     // versionCount
          + CellDescriptor.serializedSize();  // CellDescriptor for key
    }

    int getVersionCount() {
      return versionCount;
    }

    /**
     * Obtains the key value using the {@link ValuePoolReader} provided.
     *
     * @param poolReader the {@code ValuePoolReader} through which the key value is obtained
     *
     * @return the key value for this record
     *
     * @throws IOException if an error is raised while extracting the key
     * @throws BufferUnderflowException if the buffer is too small (truncated) for the encoded key
     */
    K getKey(final ValuePoolReader poolReader) throws IOException, BufferUnderflowException {
      if (this.key != null) {
        return this.key;
      }

      this.key = poolReader.readKey(this.keyDescriptor);
      return this.key;
    }

    /**
     * Appends the header data to the {@code ByteBuffer} instance provided.  This method uses
     * <i>relative</i> {@code ByteBuffer} methods and affects the position of the buffer supplied.
     *
     * @param buffer the {@code ByteBuffer} to which the header is appended.
     *
     * @throws IOException if an error is raised while writing the key
     * @throws BufferOverflowException if the buffer is too small to hold the record header and key
     */
    void put(final ByteBuffer buffer, final ValuePoolWriter poolWriter) throws IOException, BufferOverflowException {
      super.put(buffer);
      buffer.putInt(this.versionCount);

      /*
       * Write out the implied cell descriptor for the key.
       */
      poolWriter.writeKey(this.key).put(buffer);
    }
  }

  /**
   * Extends {@link RecordCodec.RecordSize} to add the {@link RecordDescriptorPool} size.
   */
  public static final class RecordSize extends RecordCodec.RecordSize {

    private final int recordDescriptorPoolSize;

    public RecordSize(final int headerSize, final int recordDescriptorPoolSize, final int valuePoolSize) {
      super(headerSize, valuePoolSize);
      this.recordDescriptorPoolSize = recordDescriptorPoolSize;
    }

    public int getRecordDescriptorPoolSize() {
      return recordDescriptorPoolSize;
    }

    @Override
    public int getRecordSize() {
      return super.getRecordSize() + this.recordDescriptorPoolSize;
    }
  }
}
