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
import com.terracottatech.sovereign.impl.memory.SingleRecord;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.common.valuepile.RandomValuePileReader;
import com.terracottatech.sovereign.common.valuepile.ValuePileReader;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Adapted from SimpleRecordBufferStrategy
 *
 * @author cschanck
 **/
class ValuePileBufferReader<K extends Comparable<K>> {

  private final DatasetSchemaImpl schema;
  private final Type<K> keyType;
  private final ValuePileReader reader = ValuePileReader.reader(0);
  private final boolean lazily;

  public ValuePileBufferReader(Type<K> keyType, DatasetSchemaImpl schema, boolean lazily) {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(keyType);
    this.schema = schema;
    this.lazily = lazily;
    this.keyType = keyType;
  }

  public SovereignPersistentRecord<K> fromByteBuffer(SovereignDataSetConfig<K, ?> config,
                                                     ByteBuffer buf) throws IOException {
    if (lazily) {
      reader.reset(buf);
      ValuePileReader rdr = reader.dup(false);
      SovereignPersistentRecord<K> rec = readLazyRecord(config, schema, rdr);
      return rec;
    } else {
      VersionedRecord<K> record = new VersionedRecord<>();
      reader.reset(buf);
      readVersionedRecord(config, record);
      return record;
    }
  }

  public K readKey(ByteBuffer buf) {
    reader.reset(buf);
    // read key
    reader.encodedInt();
    @SuppressWarnings("unchecked")
    K key = (K) readCellValueOnly(reader, keyType);
    return key;
  }

  static <K extends Comparable<K>> SovereignPersistentRecord<K> readLazyRecord(SovereignDataSetConfig<K, ?> config,
                                                  DatasetSchemaImpl schema,
                                                  ValuePileReader reader) throws IOException {
    int numRecs = reader.encodedInt();
    int keyOffset = reader.getNextFieldIndex();
    reader.skipField();

    @SuppressWarnings({"unchecked", "rawtypes"})
    LazyValuePileSingleRecord<K>[] arr = new LazyValuePileSingleRecord[numRecs];
    for (int i = 0; i < numRecs; i++) {
      LazyValuePileSingleRecord<K> r = readSingleRecordLazily(config, schema, reader);
      arr[i] = r;
    }
    return new LazyValuePileVersionedRecord<>(reader, schema, config.getType(), keyOffset, arr);
  }

  VersionedRecord<K> readVersionedRecord(SovereignDataSetConfig<K, ?> config, VersionedRecord<K> parent) {
    int numRecs = reader.encodedInt();
    @SuppressWarnings("unchecked")
    K key = (K) readCellValueOnly(reader, keyType);

    for (int i = 0; i < numRecs; i++) {
      SingleRecord<K> rec = readSingleRecord(config, key, parent);
      parent.elements().add(rec);
    }
    return parent;
  }

  SingleRecord<K> readSingleRecord(SovereignDataSetConfig<K, ?> config, K key, VersionedRecord<K> parent) {
    try {
      TimeReference<?> timeReference = readTimeReference(config, reader);
      long msn = reader.oneLong();
      int numCells = reader.encodedInt();
      Cell<?>[] arr = new Cell<?>[numCells];

      for (int i = 0; i < numCells; i++) {
        Cell<?> cell = readCell();
        arr[i] = cell;
      }
      return new SingleRecord<>(parent, key, timeReference, msn, arr);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static <K extends Comparable<K>> LazyValuePileSingleRecord<K> readSingleRecordLazily(SovereignDataSetConfig<?, ?> config,
                                                          DatasetSchemaImpl schema,
                                                          ValuePileReader reader) throws IOException {
    TimeReference<?> timeReference = readTimeReference(config, reader);
    long msn = reader.oneLong();
    int numCells = reader.encodedInt();
    LazyCell<?>[] arr = new LazyCell<?>[numCells];

    LazyValuePileSingleRecord<K> rec = new LazyValuePileSingleRecord<>(msn, timeReference);
    for (int i = 0; i < numCells; i++) {
      LazyCell<?> lc = readCellLazily(rec, schema, reader);
      arr[i] = lc;
    }
    rec.setLazyArray(arr);
    return rec;
  }

  private static TimeReference<?> readTimeReference(SovereignDataSetConfig<?, ?> config,
                                                 ValuePileReader reader) throws IOException {
    final ByteBuffer buffer = reader.bytes();
    try {
      return config.getTimeReferenceGenerator().get(buffer);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  Cell<?> readCell() {
    // is it encoded
    if (reader.oneByte() == ValuePileBufferWriter.ENCODED_CD_FLAG) {
      int sid = reader.encodedInt();
      CellDefinition<Object> def = (CellDefinition<Object>) schema.definitionFor(sid);
      Object val = readCellValueOnly(reader, def.type());
      return def.newCell(val);
    } else {
      int ordinal = reader.oneByte();
      Type<?> type = Type.fromOrdinal(ordinal);
      CellDefinition<Object> def = CellDefinition.define(reader.utfString(), (Type<Object>) type);
      Object val = readCellValueOnly(reader, type);
      return def.newCell(val);
    }
  }

  static LazyCell<?> readCellLazily(LazyValuePileSingleRecord<?> rec, DatasetSchemaImpl schema, ValuePileReader reader) {
    int defIndex = reader.getNextFieldIndex();
    if (reader.oneByte() == ValuePileBufferWriter.ENCODED_CD_FLAG) {
      reader.skipField();
      int valIndex = reader.getNextFieldIndex();
      reader.skipField();
      return new LazyCell<>(rec, defIndex, valIndex);
    } else {
      reader.skipField().skipField();
      int valIndex = reader.getNextFieldIndex();
      reader.skipField();
      return new LazyCell<>(rec, defIndex, valIndex);
    }
  }

  public static Object readCellValueOnly(ValuePileReader reader, Type<?> type) {
    switch (type.asEnum()) {
      case BOOL:
        return reader.oneBoolean();
      case CHAR:
        return reader.oneChar();
      case STRING:
        return reader.utfString();
      case INT:
        return reader.oneInt();
      case LONG:
        return reader.oneLong();
      case DOUBLE:
        return reader.oneDouble();
      case BYTES:
        ByteBuffer b = reader.bytes();
        byte[] barr = new byte[b.remaining()];
        b.get(barr);
        return barr;
      default:
        throw new RuntimeException("bad type");
    }
  }

  public static Object readCellValueOnly(RandomValuePileReader reader, Type<?> type, int index) {
    switch (type.asEnum()) {
      case BOOL:
        return reader.oneBoolean(index);
      case CHAR:
        return reader.oneChar(index);
      case STRING:
        return reader.utfString(index);
      case INT:
        return reader.oneInt(index);
      case LONG:
        return reader.oneLong(index);
      case DOUBLE:
        return reader.oneDouble(index);
      case BYTES:
        ByteBuffer b = reader.bytes(index);
        byte[] barr = new byte[b.remaining()];
        b.get(barr);
        return barr;
      default:
        throw new RuntimeException("bad type");
    }
  }

  public static CellDefinition<?> readCellDefinition(RandomValuePileReader reader, DatasetSchemaImpl schema, int index) {
    // is it encoded
    if (reader.oneByte(index++) == ValuePileBufferWriter.ENCODED_CD_FLAG) {
      int sid = reader.encodedInt(index++);
      CellDefinition<?> def = schema.definitionFor(sid);
      return def;
    } else {
      int ordinal = reader.oneByte(index++);
      Type<?> type = Type.fromOrdinal(ordinal);
      CellDefinition<?> def = CellDefinition.define(reader.utfString(index), type);
      return def;
    }
  }
}