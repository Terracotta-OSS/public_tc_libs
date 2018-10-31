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
package org.terracotta.catalog.msgs;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.EnumMappingBuilder;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.ArrayDecoder;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.ArrayEncoder;
import org.terracotta.runnel.encoding.StructEncoder;
import org.terracotta.runnel.encoding.StructEncoderFunction;

/**
 *
 */
public class CatalogCodec implements MessageCodec<CatalogReq, CatalogRsp> {
  private static final Struct RECONNECT_STRUCT = StructBuilder.newStructBuilder()
          .strings("keys", 1)
          .build();


  private static final EnumMapping<?> CATALOG_CMDS = EnumMappingBuilder.newEnumMappingBuilder(CatalogCommand.class)
          .mapping(CatalogCommand.CHECK_LEADER, 1)
          .mapping(CatalogCommand.MAKE_LEADER, 2)
          .mapping(CatalogCommand.RELEASE_LEADER, 3)
          .mapping(CatalogCommand.LOCK, 4)
          .mapping(CatalogCommand.UNLOCK, 5)
          .mapping(CatalogCommand.REFERENCE, 6)
          .mapping(CatalogCommand.RELEASE, 7)
          .mapping(CatalogCommand.STORE_CONFIG, 8)
          .mapping(CatalogCommand.GET_CONFIG, 9)
          .mapping(CatalogCommand.REMOVE_CONFIG, 10)
          .mapping(CatalogCommand.SYNC_CONFIG, 11)
          .mapping(CatalogCommand.LIST_ALL, 12)
          .mapping(CatalogCommand.LIST_BY_TYPE, 13)
          .build();

  private static final Struct REQUEST_STRUCT = StructBuilder.newStructBuilder()
          .enm("cmd", 1, CATALOG_CMDS)
          .string("type", 2)
          .string("name", 3)
          .byteBuffer("payload", 4)
          .build();

  private static final Struct RESPONSE_STRUCT = StructBuilder.newStructBuilder()
          .bool("result", 1)
          .byteBuffer("payload", 2)
          .build();

  private static final Struct MAP_ENTRY_STRUCT = StructBuilder.newStructBuilder()
          .string("key", 10)
          .string("valueString", 20)
          .byteBuffer("valueBytes", 30)
          .build();

  private static final Struct MAP_STRUCT = StructBuilder.newStructBuilder()
          .structs("entries", 10, MAP_ENTRY_STRUCT)
          .build();

  @Override
  public byte[] encodeMessage(CatalogReq message) throws MessageCodecException {
    StructEncoder<Void> encode =  REQUEST_STRUCT.encoder();
    encode.enm("cmd", message.getCmd());
    switch (message.getCmd()) {
      case LOCK:
      case UNLOCK:
      case GET_CONFIG:
      case REMOVE_CONFIG:
      case LIST_BY_TYPE:
        encode.string("type", message.getType()).string("name", message.getName());
        encode.byteBuffer("payload", ByteBuffer.wrap(new byte[] {}));
        break;
      case STORE_CONFIG:
        encode.string("type", message.getType()).string("name", message.getName());
        encode.byteBuffer("payload", ByteBuffer.wrap(message.getPayload()));
        break;
      case LIST_ALL:
        break;
      default:
        throw new AssertionError();
    }
    ByteBuffer buffer = encode.encode();
    buffer.flip();
    byte[] buf = new byte[buffer.remaining()];
    buffer.get(buf);
    return buf;
  }

  @Override
  public CatalogReq decodeMessage(byte[] payload) throws MessageCodecException {
    StructDecoder<?> decode = REQUEST_STRUCT.decoder(ByteBuffer.wrap(payload));
    CatalogCommand cmd = (CatalogCommand)decode.enm("cmd").get();
    String type = null;
    String name = null;
    byte[] data = null;
    switch (cmd) {
      case LOCK:
      case UNLOCK:
      case GET_CONFIG:
      case REMOVE_CONFIG:
      case STORE_CONFIG:
      case LIST_BY_TYPE:
        type = decode.string("type");
        name = decode.string("name");
        ByteBuffer pay = decode.byteBuffer("payload");
        data = new byte[pay.remaining()];
        pay.get(data);
        break;
      case LIST_ALL:
        break;
      default:
        throw new AssertionError("bad command type " + cmd);
    }
    return new CatalogReq(cmd, type, name, data);
  }

  @Override
  public byte[] encodeResponse(CatalogRsp response) throws MessageCodecException {
    StructEncoder<?> encoder = RESPONSE_STRUCT.encoder();
    encoder.bool("result", response.result());
    byte[] payload = response.getPayload();
    if (payload != null) {
      encoder.byteBuffer("payload", ByteBuffer.wrap(payload));
    }
    ByteBuffer buffer = encoder.encode();
    buffer.flip();
    byte[] raw = new byte[buffer.remaining()];
    buffer.get(raw);
    return raw;
  }

  @Override
  public CatalogRsp decodeResponse(byte[] payload) throws MessageCodecException {
    StructDecoder<?> decode = RESPONSE_STRUCT.decoder(ByteBuffer.wrap(payload));
    return new CatalogRsp(decode.bool("result"), decode.byteBuffer("payload"));
  }

  public static byte[] encodeReconnect(String[] keys) {
    StructEncoder<Void> encode = RECONNECT_STRUCT.encoder();
    ArrayEncoder<String, StructEncoder<Void>> list = encode.strings("keys");
    for (String key : keys) {
      list.value(key);
    }
    encode = list.end();
    ByteBuffer buffer = encode.encode();
    buffer.flip();
    byte[] raw = new byte[buffer.remaining()];
    buffer.get(raw);
    return raw;
  }

  public static String[] decodeReconnect(byte[] data) {
    StructDecoder<Void> decoder = RECONNECT_STRUCT.decoder(ByteBuffer.wrap(data));
    ArrayDecoder<String, StructDecoder<Void>> decode = decoder.strings("keys");
    int len = decode.length();
    String[] keys = new String[len];
    for (int x=0;x<len;x++) {
      keys[x] = decode.value();
    }
    return keys;
  }

  public static byte[] encodeMapStringString(Map<String, String> map) {
    StructEncoder<Void> mapEncoder = MAP_STRUCT.encoder();

    mapEncoder.structs("entries", map.entrySet(), new StructEncoderFunction<Map.Entry<String, String>>() {
      @Override
      public void encode(StructEncoder<?> entryEncoder, Map.Entry<String, String> stringStringEntry) {
        entryEncoder.string("key", stringStringEntry.getKey());
        entryEncoder.string("valueString", stringStringEntry.getValue());
      }
    });

    return mapEncoder.encode().array();
  }

  public static Map<String, String> decodeMapStringString(byte[] bytes) {
    Map<String, String> result = new HashMap<String, String>();
    StructDecoder<Void> mapDecoder = MAP_STRUCT.decoder(ByteBuffer.wrap(bytes));

    StructArrayDecoder<?> entriesDecoder = mapDecoder.structs("entries");
    while (entriesDecoder.hasNext()) {
      StructDecoder<?> entryDecoder = entriesDecoder.next();
      String key = entryDecoder.string("key");
      String value = entryDecoder.string("valueString");
      result.put(key, value);
    }

    return result;
  }

  public static byte[] encodeMapStringByteArray(Map<String, byte[]> map) {
    StructEncoder<Void> mapEncoder = MAP_STRUCT.encoder();

    mapEncoder.structs("entries", map.entrySet(), new StructEncoderFunction<Map.Entry<String, byte[]>>() {
      @Override
      public void encode(StructEncoder<?> entryEncoder, Map.Entry<String, byte[]> stringStringEntry) {
        entryEncoder.string("key", stringStringEntry.getKey());
        entryEncoder.byteBuffer("valueBytes", ByteBuffer.wrap(stringStringEntry.getValue()));
      }
    });

    return mapEncoder.encode().array();
  }

  public static Map<String, byte[]> decodeMapStringByteArray(byte[] bytes) {
    Map<String, byte[]> result = new HashMap<String, byte[]>();
    StructDecoder<Void> mapDecoder = MAP_STRUCT.decoder(ByteBuffer.wrap(bytes));

    StructArrayDecoder<?> entriesDecoder = mapDecoder.structs("entries");
    while (entriesDecoder.hasNext()) {
      StructDecoder<?> entryDecoder = entriesDecoder.next();
      String key = entryDecoder.string("key");
      ByteBuffer valueBuffer = entryDecoder.byteBuffer("valueBytes");
      byte[] value = new byte[valueBuffer.remaining()];
      valueBuffer.get(value);
      result.put(key, value);
    }

    return result;
  }

}
