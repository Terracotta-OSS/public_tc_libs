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

package com.terracottatech.store.common.messages;

import com.terracottatech.store.Cell;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import com.terracottatech.store.intrinsics.Intrinsic;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static com.terracottatech.store.common.messages.StoreStructures.decodeCells;
import static com.terracottatech.store.common.messages.StoreStructures.decodeKey;

/**
 * Created by cdennis on 2/9/17.
 */
public class DatasetStructDecoder {

  private final StructDecoder<?> underlying;
  private final IntrinsicCodec intrinsicCodec;
  private final ThrowableCodec throwableCodec;

  public DatasetStructDecoder(StructDecoder<?> decoder, IntrinsicCodec intrinsicCodec, ThrowableCodec throwableCodec) {
    this.underlying = decoder;
    this.intrinsicCodec = intrinsicCodec;
    this.throwableCodec = throwableCodec;
  }

  public Intrinsic intrinsic(String name) {
    return intrinsicCodec.decodeIntrinsic(underlying.struct(name));
  }

  public <K extends Comparable<K>> K key(String name) {
    return decodeKey(underlying.struct(name));
  }

  public Boolean bool(String name) {
    return underlying.bool(name);
  }

  public List<Cell<?>> cells(String name) {
    return decodeCells(underlying.structs(name));
  }

  public UUID uuid(String name) {
    StructDecoder<?> decoder = underlying.struct(name);
    if (decoder == null) {
      return null;
    } else {
      UUID uuid = StoreStructures.decodeUUID(decoder);
      decoder.end();
      return uuid;
    }
  }

  public Collection<UUID> uuids(String name) {
    StructArrayDecoder<? extends StructDecoder<?>> structs = underlying.structs(name);

    if (structs == null) {
      return null;
    }

    int uuidCount = structs.length();
    List<UUID> uuids = new ArrayList<>(uuidCount);

    structs.forEachRemaining(decoder -> uuids.add(StoreStructures.decodeUUID(decoder)));

    return uuids;
  }

  public Throwable throwable(String name) {
    StructDecoder<? extends StructDecoder<?>> structDecoder = underlying.struct(name);
    return structDecoder == null ? null : throwableCodec.decode(structDecoder);
  }

  public <T> Enm<T> enm(String name) {
    return underlying.enm(name);
  }

  public StructDecoder<?> getUnderlying() {
    return underlying;
  }

  public <T> T struct(String name, Function<DatasetStructDecoder, T> function) {
    StructDecoder<?> decoder = underlying.struct(name);
    if (decoder == null) {
      return null;
    } else {
      return function.apply(new DatasetStructDecoder(decoder, intrinsicCodec, throwableCodec));
    }
  }

  public <T> List<T> structs(String name, Function<DatasetStructDecoder, T> function) {
    List<T> values = new ArrayList<>();
    StructArrayDecoder<?> subStructArrayEncoder = underlying.structs(name);
    while (subStructArrayEncoder.hasNext()) {
      values.add(function.apply(new DatasetStructDecoder(subStructArrayEncoder.next(), intrinsicCodec, throwableCodec)));
    }
    subStructArrayEncoder.end();
    return values;
  }

  public List<Intrinsic> intrinsics(String name) {
    return structs(name, ds -> intrinsicCodec.decodeIntrinsic(ds.getUnderlying()));
  }

  public Double fp64(String name) {
    return underlying.fp64(name);
  }

  public Integer int32(String name) {
    return underlying.int32(name);
  }

  public Long int64(String name) {
    return underlying.int64(name);
  }

  public ByteBuffer byteBuffer(String name) {
    return underlying.byteBuffer(name);
  }

  public <K extends Comparable<K>> RecordData<K> record(String name) {
    StructDecoder<?> record = underlying.struct(name);
    if (record == null) {
      return null;
    } else {
      long msn = record.int64("msn");
      StructDecoder<?> keyStructDecoder = record.struct("key");
      K key = decodeKey(keyStructDecoder);
      keyStructDecoder.end();
      Collection<Cell<?>> cells = decodeCells(record.structs("cells"));
      record.end();
      return new RecordData<>(msn, key, cells);
    }
  }

  public String string(String name) {
    return underlying.string(name);
  }
}
