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

import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.runnel.EnumMappingBuilder;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.Map;

public class AbstractMessageCodec implements MessageCodec<DatasetEntityMessage, DatasetEntityResponse> {

  private final Struct messageStruct;
  private final Struct responseStruct;
  private final IntrinsicCodec intrinsicCodec;
  private final ThrowableCodec throwableCodec;

  public <M extends MessageComponent<?>, R extends MessageComponent<?>> AbstractMessageCodec(IntrinsicCodec intrinsicCodec,
                                                                                             Class<M> messageType, Map<Integer, M> messages,
                                                                                             Class<R> responseType, Map<Integer, R> responses) {
    this.intrinsicCodec = intrinsicCodec;
    this.throwableCodec = new ThrowableCodec();
    this.messageStruct = buildCompositeStruct(messageType, messages, intrinsicCodec.intrinsicStruct(), throwableCodec.getThrowableStruct());
    this.responseStruct = buildCompositeStruct(responseType, responses, intrinsicCodec.intrinsicStruct(), throwableCodec.getThrowableStruct());
  }

  private static <T extends MessageComponent<?>> Struct buildCompositeStruct(Class<T> messageType, Map<Integer, T> components, Struct intrinsicStruct, Struct throwableStruct) {
    EnumMappingBuilder<T> typeMapping = EnumMappingBuilder.newEnumMappingBuilder(messageType);
    for (Map.Entry<Integer, T> component : components.entrySet()) {
      typeMapping.mapping(component.getValue(), component.getKey());
    }

    StructBuilder builder = StructBuilder.newStructBuilder().enm("messageType", 10, typeMapping.build());
    for (Map.Entry<Integer, T> component : components.entrySet()) {
      builder.struct(component.getValue().toString(), 20 + component.getKey(), component.getValue().struct(new DatasetStructBuilder(intrinsicStruct, throwableStruct)));
    }
    return builder.build();
  }

  @Override
  public byte[] encodeMessage(DatasetEntityMessage datasetEntityMessage) throws MessageCodecException {
    try {
      @SuppressWarnings("unchecked")
      MessageComponent<DatasetEntityMessage> type = (MessageComponent<DatasetEntityMessage>) datasetEntityMessage.getType();
      StructEncoder<Void> encoder = messageStruct.encoder().enm("messageType", type);
      type.encode(new DatasetStructEncoder(encoder.struct(type.name()), intrinsicCodec, throwableCodec), datasetEntityMessage);
      return encoder.encode().array();
    } catch (RuntimeException e) {
      throw new MessageCodecException("Error encoding message. Please check your client and server versions.", e);
    }
  }

  @Override
  public DatasetEntityMessage decodeMessage(byte[] bytes) throws MessageCodecException {
    try {
      StructDecoder<Void> decoder = messageStruct.decoder(ByteBuffer.wrap(bytes));
      MessageComponent<? extends DatasetEntityMessage> type = decoder.<MessageComponent<? extends DatasetEntityMessage>>enm("messageType").get();
      return type.decode(new DatasetStructDecoder(decoder.struct(type.name()), intrinsicCodec, throwableCodec));
    } catch (RuntimeException e) {
      throw new MessageCodecException("Error decoding message. Please check your client and server versions.", e);
    }
  }

  @Override
  public byte[] encodeResponse(DatasetEntityResponse datasetEntityResponse) throws MessageCodecException {
    try {
      DatasetEntityResponseType type = datasetEntityResponse.getType();
      StructEncoder<Void> encoder = responseStruct.encoder().enm("messageType", type);
      type.encode(new DatasetStructEncoder(encoder.struct(type.name()), intrinsicCodec, throwableCodec), datasetEntityResponse);
      return encoder.encode().array();
    } catch (RuntimeException e) {
      throw new MessageCodecException("Error encoding response. Please check your client and server versions.", e);
    }
  }

  @Override
  public DatasetEntityResponse decodeResponse(byte[] bytes) throws MessageCodecException {
    try {
      StructDecoder<Void> decoder = responseStruct.decoder(ByteBuffer.wrap(bytes));
      DatasetEntityResponseType type = decoder.<DatasetEntityResponseType>enm("messageType").get();
      return type.decode(new DatasetStructDecoder(decoder.struct(type.name()), intrinsicCodec, throwableCodec));
    } catch (RuntimeException e) {
      throw new MessageCodecException("Error decoding response. Please check your client and server versions.", e);
    }
  }
}
