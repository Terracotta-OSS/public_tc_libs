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
package com.terracottatech.testing.calculator;

import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.EnumMappingBuilder;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;

/**
 *
 */
public class CalculatorCodec implements MessageCodec<CalculatorReq, CalculatorRsp> {

  private static final EnumMapping<CalculatorReq.Operation> OPERATION_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(CalculatorReq.Operation.class)
      .mapping(CalculatorReq.Operation.ADDITION, 0)
      .build();

  private static final Struct REQ_STRUCT = StructBuilder.newStructBuilder()
      .enm("operation", 10, OPERATION_MAPPING)
      .int32("x", 20)
      .int32("y", 30)
      .build();

  private static final Struct RSP_STRUCT = StructBuilder.newStructBuilder()
      .int32("result", 10)
      .build();

  private static final Struct CONFIG_STRUCT = StructBuilder.newStructBuilder()
      .bool("null", 10)
      .string("brand", 20)
      .build();


  @Override
  public byte[] encodeMessage(CalculatorReq message) throws MessageCodecException {
    StructEncoder<Void> encoder = REQ_STRUCT.encoder();

    encoder.enm("operation", message.getOperation());
    encoder.int32("x", message.getX());
    encoder.int32("y", message.getY());

    return encoder.encode().array();
  }

  @Override
  public CalculatorReq decodeMessage(byte[] payload) throws MessageCodecException {
    StructDecoder<Void> decoder = REQ_STRUCT.decoder(ByteBuffer.wrap(payload));

    Enm<CalculatorReq.Operation> opEnum = decoder.enm("operation");
    CalculatorReq.Operation operation = opEnum.get();
    Integer x = decoder.int32("x");
    Integer y = decoder.int32("y");

    return new CalculatorReq(x, y, operation);
  }

  @Override
  public byte[] encodeResponse(CalculatorRsp response) throws MessageCodecException {
    StructEncoder<Void> encoder = RSP_STRUCT.encoder();

    encoder.int32("result", response.getResult());

    return encoder.encode().array();
  }

  @Override
  public CalculatorRsp decodeResponse(byte[] payload) throws MessageCodecException {
    StructDecoder<Void> decoder = RSP_STRUCT.decoder(ByteBuffer.wrap(payload));

    Integer result = decoder.int32("result");

    return new CalculatorRsp(result);
  }

  public static byte[] serializeConfiguration(CalculatorConfig configuration) {
    StructEncoder<Void> encoder = CONFIG_STRUCT.encoder();

    if (configuration == null) {
      encoder.bool("null", true);
    } else {
      encoder.bool("null", false);
      if (configuration.getBrand() != null) {
        encoder.string("brand", configuration.getBrand());
      }
    }

    return encoder.encode().array();
  }

  public static CalculatorConfig deserializeConfiguration(byte[] configuration) {
    StructDecoder<Void> decoder = CONFIG_STRUCT.decoder(ByteBuffer.wrap(configuration));

    boolean nullConf = decoder.bool("null");
    if (nullConf) {
      return null;
    } else {
      String brand = decoder.string("brand");
      return new CalculatorConfig(brand);
    }
  }

}
