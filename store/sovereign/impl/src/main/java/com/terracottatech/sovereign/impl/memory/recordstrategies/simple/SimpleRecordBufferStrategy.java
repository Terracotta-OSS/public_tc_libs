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


package com.terracottatech.sovereign.impl.memory.recordstrategies.simple;

import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaImpl;
import com.terracottatech.sovereign.impl.dataset.metadata.SchemaCellDefinition;
import com.terracottatech.sovereign.impl.memory.RecordBufferStrategy;
import com.terracottatech.sovereign.impl.memory.SingleRecord;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.terracottatech.sovereign.common.utils.NIOBufferUtils.ByteBufferOutputStream;

/**
 * Added some cleverness to reuse input/output streams, But to make sure buffers are not
 * left lying around, so the thread local impact is small.
 *
 * @author cschanck
 **/
public class SimpleRecordBufferStrategy<K extends Comparable<K>>
  implements RecordBufferStrategy<K> {

  private final static byte CELLDEF_IDS_ONLY = (byte) 1;
  private final static byte CELLDEF_BYNAME = (byte) 0;
  private final SovereignDataSetConfig<K, ?> config;
  private final AtomicInteger timeReferencedMaximumSerializedLength = new AtomicInteger();
  private final DatasetSchemaImpl schema;
  private final Type<K> keyType;

  private final ThreadLocal<ReusableDataOutputStream<ByteBufferOutputStream>> threadROS;
  private final ThreadLocal<ReusableDataInputStream<NIOBufferUtils.ByteBufferInputStream>> threadRIS;

  public SimpleRecordBufferStrategy(final SovereignDataSetConfig<K, ?> config, DatasetSchemaImpl schema) {
    this.config = config;
    this.schema = schema;
    this.timeReferencedMaximumSerializedLength.set(this.config.getTimeReferenceGenerator()
                                                     .maxSerializedLength());
    this.keyType = config.getType();
    this.threadROS = ThreadLocal.withInitial(dataOutputStreamSupplier());
    this.threadRIS = ThreadLocal.withInitial(dataInputStreamSupplier());
  }

  private static Supplier<ReusableDataOutputStream<ByteBufferOutputStream>> dataOutputStreamSupplier() {
    return () -> new ReusableDataOutputStream<>(new ByteBufferOutputStream());
  }

  private static Supplier<ReusableDataInputStream<NIOBufferUtils.ByteBufferInputStream>> dataInputStreamSupplier() {
    return () -> new ReusableDataInputStream<>(new NIOBufferUtils.ByteBufferInputStream(null));
  }

  @Override
  public ByteBuffer toByteBuffer(SovereignPersistentRecord<K> record) {
    try {
      ReusableDataOutputStream<ByteBufferOutputStream> ros = threadROS.get();
      ros.reuse();
      ros.getCurrentStream().reuse();
      writeVersionedRecord(ros, record);
      ros.flush();
      ByteBuffer ret = ros.getCurrentStream().takeBuffer();
      return ret;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public VersionedRecord<K> fromByteBuffer(ByteBuffer buf) {
    ReusableDataInputStream<NIOBufferUtils.ByteBufferInputStream> ris = threadRIS.get();
    ris.getCurrentStream().reuse(buf.slice());
    ris.reuse();
    VersionedRecord<K> record = new VersionedRecord<>();
    readVersionedRecord(ris, record);
    ris.getCurrentStream().reuse(null);
    return record;
  }

  @Override
  public K readKey(ByteBuffer buf) {
    ReusableDataInputStream<NIOBufferUtils.ByteBufferInputStream> ris = threadRIS.get();
    ris.getCurrentStream().reuse(buf.slice());
    ris.reuse();
    try {
      int numRecs = ris.readInt();
      @SuppressWarnings("unchecked")
      K ret = (K) readCellValueOnly(ris, keyType);
      ris.getCurrentStream().reuse(null);
      return ret;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeVersionedRecord(DataOutput out, SovereignPersistentRecord<K> record) {
    try {
      out.writeInt(record.elements().size());
      for (SovereignPersistentRecord<K> sr : record.elements()) {
        writeSingleRecord(out, sr);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeSingleRecord(DataOutput out, SovereignPersistentRecord<K> record) {
    try {
      writeKey(out, record.getKey());
      this.writeTimeReference(out, record.getTimeReference());
      out.writeLong(record.getMSN());
      out.writeShort(record.cells().size());
      for (Cell<?> cell : record.cells().values()) {
        writeCellValue(out, cell);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeTimeReference(final DataOutput out, final TimeReference<?> timeReference) throws IOException {
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
        TimeReferenceGenerator<?> gen = this.config.getTimeReferenceGenerator();
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

    out.writeInt(serializedLength);
    if (serializedLength > 0) {
      out.write(bytes, 0, serializedLength);
    }
  }

  void writeKey(DataOutput out, K key) {
    try {
      writeCellValueOnly(out, keyType ,key);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeCellValue(DataOutput out, Cell<?> cell) {
    try {
      if (cell.definition().type() == null) {
        throw new AssertionError("invalid type");
      }
      writePackedDefinition(schema, out, cell.definition());
      writeCellValueOnly(out, cell.definition().type(), cell.value());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private void writeCellValueOnly(DataOutput out, Type<?> t, Object value) throws IOException {
    switch (t.asEnum()) {
      case BOOL:
        out.writeBoolean((Boolean) value);
        break;
      case CHAR:
        out.writeChar((Character) value);
        break;
      case STRING:
        out.writeUTF(value.toString());
        break;
      case INT:
        out.writeInt((Integer) value);
        break;
      case LONG:
        out.writeLong((Long) value);
        break;
      case DOUBLE:
        out.writeDouble((Double) value);
        break;
      case BYTES:
        out.writeInt(((byte[]) value).length);
        out.write((byte[]) value);
        break;
      default:
        throw new RuntimeException("bad type");
    }
  }

  VersionedRecord<K> readVersionedRecord(DataInput in, VersionedRecord<K> version) {
    try {
      int numRecs = in.readInt();
      for (int i = 0; i < numRecs; i++) {
        SingleRecord<K> rec = readSingleRecord(in, version);
        version.elements().add(rec);
      }
      return version;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public SingleRecord<K> readSingleRecord(DataInput in, VersionedRecord<K> parent) {
    try {
      @SuppressWarnings("unchecked") K key = (K) readCellValueOnly(in, keyType);
      TimeReference<?> timeReference = this.readTimeReference(in);
      long msn = in.readLong();
      int numCells = in.readShort();
      Cell<?>[] arr = new Cell<?>[numCells];

      for (int i = 0; i < numCells; i++) {
        Cell<?> cell = readCellValue(in);
        arr[i] = cell;
      }
      return new SingleRecord<>(parent, key, timeReference, msn, arr);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private TimeReference<?> readTimeReference(final DataInput in) throws IOException {
    final int serializedLength = in.readInt();
    final byte[] bytes = new byte[serializedLength];
    in.readFully(bytes);

    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    try {
      return this.config.getTimeReferenceGenerator().get(buffer);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public <T> Cell<?> readCellValue(DataInput in) {
    try {
      CellDefinition<T> def = (CellDefinition<T>) readPackedDefinition(schema, in);
      T val = (T) readCellValueOnly(in, def.type());
      return def.newCell(val);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  Object readCellValueOnly(DataInput in, Type<?> type) throws IOException {
    switch (type.asEnum()) {
      case BOOL:
        return in.readBoolean();
      case CHAR:
        return in.readChar();
      case STRING:
        return in.readUTF();
      case INT:
        return in.readInt();
      case LONG:
        return in.readLong();
      case DOUBLE:
        return in.readDouble();
      case BYTES:
        int len = in.readInt();
        byte[] b = new byte[len];
        in.readFully(b);
        return b;
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

  static class ReusableDataInputStream<I extends InputStream> extends DataInputStream {

    /**
     * Creates a DataInputStream that uses the specified
     * underlying InputStream.
     *
     * @param input the specified input stream
     */
    public ReusableDataInputStream(I input) {
      super(input);
    }

    @SuppressWarnings("unchecked")
    public I getCurrentStream() {
      return (I) this.in;
    }

    public void reuse(I input) {
      this.in = input;
    }

    public void reuse() {
      // noop
    }
  }

  // these use the top 2 bits to encode the type of packing.
  private final static int PACKED_NO_ID = 0x00;
  private final static int PACKED_6BITS = 0x40;
  private final static int PACKED_SHORT_ID = 0x80;
  private final static int PACKED_INT_ID = 0xc0;

  public static void writePackedDefinition(DatasetSchemaImpl schema,
                                           DataOutput output,
                                           CellDefinition<?> def) throws IOException {
    if (schema == null) {
      output.writeByte(def.type().asEnum().ordinal());
      output.writeUTF(def.name());
    } else {
      SchemaCellDefinition<?> scd = schema.idFor(def);
      if (scd == null) {
        output.writeByte(PACKED_NO_ID);
        output.writeByte(def.type().asEnum().ordinal());
        output.writeUTF(def.name());
      } else if (scd.id() < 64) {
        int hdr = PACKED_6BITS | scd.id();
        output.writeByte(hdr);
      } else if (scd.id() < Short.MAX_VALUE) {
        output.writeByte(PACKED_SHORT_ID);
        output.writeShort(scd.id());
      } else {
        output.writeByte(PACKED_INT_ID);
        output.writeInt(scd.id());
      }
    }
  }

  public static CellDefinition<?> readPackedDefinition(DatasetSchemaImpl schema,
                                                            DataInput input) throws IOException {
    if (schema == null) {
      int ordinal = input.readUnsignedByte();
      Type<?> type = Type.fromOrdinal(ordinal);
      return CellDefinition.define(input.readUTF(), type);
    } else {
      int hdr = input.readUnsignedByte();
      int mask = hdr & 0xc0;
      switch (mask) {
        case PACKED_NO_ID: {
          int ordinal = input.readUnsignedByte();
          Type<?> type = Type.fromOrdinal(ordinal);
          return CellDefinition.define(input.readUTF(), type);
        }
        case PACKED_6BITS: {
          hdr = hdr & 0x3f;
          return schema.definitionFor(hdr);
        }
        case PACKED_SHORT_ID: {
          short scid = input.readShort();
          return schema.definitionFor(scid);
        }
        case PACKED_INT_ID: {
          int scid = input.readInt();
          return schema.definitionFor(scid);
        }
        default:
          throw new IllegalStateException();
      }
    }
  }
}