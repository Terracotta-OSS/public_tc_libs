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
package com.terracottatech.sovereign.impl.memory.recordstrategies.valuepilecodec;

import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaImpl;
import com.terracottatech.sovereign.impl.dataset.metadata.SchemaCellDefinition;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.sovereign.common.valuepile.ValuePileWriter;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Type;

import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static com.terracottatech.sovereign.common.utils.NIOBufferUtils.ByteBufferOutputStream;

/**
 * Adapted from SimpleRecordBufferStrategy
 *
 * @author cschanck
 **/
class ValuePileBufferWriter<K extends Comparable<K>> {

  public static final byte ENCODED_CD_FLAG = (byte) 0;
  public static final byte UNENCODED_CD_FLAG = (byte) 1;
  private final AtomicInteger timeReferencedMaximumSerializedLength;
  private final DatasetSchemaImpl schema;
  private final Type<K> keyType;
  private final ReusableDataOutputStream<ByteBufferOutputStream> ros = new ReusableDataOutputStream<>(new ByteBufferOutputStream(
    8 * 1024));
  private ValuePileWriter writer = ValuePileWriter.writer(0);

  public ValuePileBufferWriter(AtomicInteger timeReferencedMaximumSerializedLength,
                               Type<K> keyType,
                               DatasetSchemaImpl schema) {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(keyType);
    this.timeReferencedMaximumSerializedLength = timeReferencedMaximumSerializedLength;
    this.schema = schema;
    this.keyType = keyType;
  }

  public ByteBuffer toByteBuffer(SovereignDataSetConfig<K, ?> config, SovereignPersistentRecord<K> record) {
    try {
      ros.reuse();
      ros.getCurrentStream().reuse();
      writer.reset(ros);
      writeVersionedRecord(config, record);
      ros.flush();
      ByteBuffer ret = ros.getCurrentStream().takeBuffer();
      return ret;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeVersionedRecord(SovereignDataSetConfig<K, ?> config, SovereignPersistentRecord<K> record) {
    try {
      writer.encodedInt(record.elements().size());
      writeKey(record.getKey());
      for (SovereignPersistentRecord<K> sr : record.elements()) {
        writeSingleRecord(config, sr);
      }
      writer.finish();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeSingleRecord(SovereignDataSetConfig<K, ?> config, SovereignPersistentRecord<K> record) {
    try {
      this.writeTimeReference(config, record.getTimeReference());
      writer.oneLong(record.getMSN());
      writer.encodedInt(record.cells().size());
      for (Cell<?> cell : record.cells().values()) {
        writeCell(cell);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeTimeReference(SovereignDataSetConfig<K, ?> config, final TimeReference<?> timeReference) throws
    IOException {
    int bufferLength = this.timeReferencedMaximumSerializedLength.get();

    byte[] bytes;
    int serializedLength = -1;
    boolean tooSmall;
    boolean lengthUpdated = false;
    do {
      tooSmall = false;
      bytes = new byte[bufferLength];
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      try {
        TimeReferenceGenerator<?> gen = config.getTimeReferenceGenerator();
        gen.put(buffer, timeReference);
        serializedLength = buffer.position();
      } catch (BufferOverflowException e) {
        bufferLength += 512;
        tooSmall = true;
        lengthUpdated = true;
      }
    } while (tooSmall);

    if (lengthUpdated) {
      this.timeReferencedMaximumSerializedLength.accumulateAndGet(serializedLength, Math::max);
    }

    // this might be zero length -- that's fine.
    writer.bytes(bytes, 0, serializedLength);
  }

  private void writeKey(K key) {
    try {
      writeCellValueOnly(keyType, key);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeCell(Cell<?> cell) throws IOException {
    SchemaCellDefinition<?> sid = schema.idFor(cell.definition());
    if (sid == null) {
      writer.oneByte(UNENCODED_CD_FLAG);
      writer.oneByte(cell.definition().type().asEnum().ordinal());
      writer.utfString(cell.definition().name());
    } else {
      writer.oneByte(ENCODED_CD_FLAG);
      writer.encodedInt(sid.id());
      writeCellValueOnly(cell.definition().type(), cell.value());
    }
  }

  private void writeCellValueOnly(Type<?> t, Object value) throws IOException {
    switch (t.asEnum()) {
      case BOOL:
        writer.oneBoolean((Boolean) value);
        break;
      case CHAR:
        writer.oneChar((Character) value);
        break;
      case STRING:
        writer.utfString(value.toString());
        break;
      case INT:
        writer.oneInt((Integer) value);
        break;
      case LONG:
        writer.oneLong((Long) value);
        break;
      case DOUBLE:
        writer.oneDouble((Double) value);
        break;
      case BYTES:
        byte[] arr = (byte[]) value;
        writer.bytes(arr, 0, arr.length);
        break;
      default:
        throw new RuntimeException("bad type");
    }
  }

  static class ReusableDataOutputStream<O extends OutputStream> extends DataOutputStream {
    /**
     * Creates a new data output stream to write data to the specified
     * underlying output stream. The counter <code>written</code> is
     * set to zero.
     *
     * @param out the underlying output stream, to be saved for later
     * use.
     * @see FilterOutputStream#out
     */
    public ReusableDataOutputStream(O out) {
      super(out);
    }

    /**
     * Reuse.
     *
     * @param newOut
     * @return
     */
    public ReusableDataOutputStream<O> reuse(O newOut) {
      reuse();
      this.out = newOut;
      return this;
    }

    public ReusableDataOutputStream<O> reuse() {
      this.written = 0;
      return this;
    }

    @SuppressWarnings("unchecked")
    public O getCurrentStream() {
      return (O) this.out;
    }
  }

}