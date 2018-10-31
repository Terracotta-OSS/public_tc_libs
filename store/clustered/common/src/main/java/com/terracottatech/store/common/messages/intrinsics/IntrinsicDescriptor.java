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
package com.terracottatech.store.common.messages.intrinsics;

import com.terracottatech.store.intrinsics.Intrinsic;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.Map;
import java.util.function.Function;


public class IntrinsicDescriptor {

  private final int typeId;
  private final String fieldName;
  private final Function<Struct, Struct> structFunction;
  private final Encoder encoder;
  private final Decoder decoder;

  public IntrinsicDescriptor(String fieldName, int typeId, Function<Struct, Struct> structFunction, Encoder encoder, Decoder decoder) {
    this.typeId = typeId;
    this.fieldName = fieldName;
    this.structFunction = structFunction;
    this.encoder = encoder;
    this.decoder = decoder;
  }

  public IntrinsicDescriptor(IntrinsicDescriptor other, Encoder encoder, Decoder decoder) {
    this.typeId = other.typeId;
    this.fieldName = other.fieldName;
    this.structFunction = other.structFunction;
    this.encoder = encoder;
    this.decoder = decoder;
  }

  public int getTypeId() {
    return typeId;
  }

  public String getFieldName() {
    return fieldName;
  }

  public Struct getStruct(Struct intrinsicStruct) {
    return structFunction.apply(intrinsicStruct);
  }

  public void encode(StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) {
    encoder.encode(intrinsicEncoder, intrinsic, intrinsicCodec);
  }

  public Intrinsic decode(StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) {
    return decoder.decode(intrinsicDecoder, intrinsicCodec);
  }

  public boolean hasDecoder() {
    return decoder != null;
  }

  public boolean hasEncoder() {
    return encoder != null;
  }

  @FunctionalInterface
  public interface Encoder {
    void encode(StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec);
  }

  @FunctionalInterface
  public interface Decoder {
    Intrinsic decode(StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec);
  }
}
