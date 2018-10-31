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
import org.terracotta.runnel.encoding.StructArrayEncoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Created by cdennis on 2/9/17.
 */
public class DatasetStructEncoder {

  private final IntrinsicCodec intrinsicCodec;
  private final ThrowableCodec throwableCodec;
  private final StructEncoder<?> underlying;

  public DatasetStructEncoder(StructEncoder<?> encoder, IntrinsicCodec intrinsicCodec, ThrowableCodec throwableCodec) {
    this.underlying = encoder;
    this.intrinsicCodec = intrinsicCodec;
    this.throwableCodec = throwableCodec;
  }

  public DatasetStructEncoder key(String name, Comparable<?> value) {
    underlying.struct(name, value, StoreStructures::encodeKey);
    return this;
  }

  public DatasetStructEncoder intrinsic(String name, Intrinsic value) {
    underlying.struct(name, value, intrinsicCodec::encodeIntrinsic);
    return this;
  }

  public DatasetStructEncoder bool(String name, boolean value) {
    underlying.bool(name, value);
    return this;
  }

  public DatasetStructEncoder cells(String name, Iterable<Cell<?>> values) {
    underlying.structs(name, values, StoreStructures::encodeCell);
    return this;
  }

  public DatasetStructEncoder uuid(String name, UUID uuid) {
    if (uuid != null) {
      underlying.struct(name, uuid, StoreStructures::encodeUUID);
    }
    return this;
  }

  public DatasetStructEncoder uuids(String name, Collection<UUID> uuids) {
    if (uuids != null) {
      underlying.structs(name, uuids, StoreStructures::encodeUUID);
    }
    return this;
  }

  public <T> DatasetStructEncoder struct(String name, T value, BiConsumer<DatasetStructEncoder, T> function) {
    StructEncoder<?> encoder = underlying.struct(name);
    function.accept(new DatasetStructEncoder(encoder, intrinsicCodec, throwableCodec), value);
    return this;
  }

  public <T> DatasetStructEncoder structs(String name, Iterable<T> values, BiConsumer<DatasetStructEncoder, T> function) {
    StructArrayEncoder<?> subStructArrayEncoder = underlying.structs(name);
    for (T value : values) {
      function.accept(new DatasetStructEncoder(subStructArrayEncoder.add(), intrinsicCodec, throwableCodec), value);
    }
    subStructArrayEncoder.end();
    return this;
  }

  public DatasetStructEncoder intrinsics(String name, Iterable<Intrinsic> values) {
    underlying.structs(name, values, intrinsicCodec::encodeIntrinsic);
    return this;
  }

  public DatasetStructEncoder throwable(String name, Throwable value) {
    underlying.struct(name, value, throwableCodec::encode);
    return this;
  }

  public DatasetStructEncoder enm(String name, Object value) {
    underlying.enm(name, value);
    return this;
  }

  public StructEncoder<?> getUnderlying() {
    return underlying;
  }

  public DatasetStructEncoder record(String name, RecordData<?> value) {
    underlying.struct(name)
            .int64("msn", value.getMsn())
            .struct("key", value.getKey(), StoreStructures::encodeKey)
            .structs("cells", value.getCells(), StoreStructures::encodeCell)
            .end();
    return this;
  }

  public DatasetStructEncoder fp64(String name, double value) {
    underlying.fp64(name, value);
    return this;
  }

  public DatasetStructEncoder int32(String name, int value) {
    underlying.int32(name, value);
    return this;
  }

  public DatasetStructEncoder int64(String name, long value) {
    underlying.int64(name, value);
    return this;
  }

  public DatasetStructEncoder byteBuffer(String name, ByteBuffer value) {
    underlying.byteBuffer(name, value);
    return this;
  }

  public DatasetStructEncoder string(String name, String value) {
    underlying.string(name, value);
    return this;
  }
}
