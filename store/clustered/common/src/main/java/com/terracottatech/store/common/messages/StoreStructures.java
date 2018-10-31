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
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.BaseDiskDurability;
import com.terracottatech.store.configuration.DiskDurability;
import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.impl.CellValue;
import com.terracottatech.store.intrinsics.impl.Constant;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.EnumMappingBuilder;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.PrimitiveDecodingSupport;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.PrimitiveEncodingSupport;
import org.terracotta.runnel.encoding.StructEncoder;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.terracottatech.store.configuration.DiskDurability.*;

public class StoreStructures {

  @SuppressWarnings("rawtypes")
  public static final EnumMapping<Type> TYPE_ENUM_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(Type.class)
      .mapping(Type.BOOL, 0)
      .mapping(Type.CHAR, 1)
      .mapping(Type.INT, 2)
      .mapping(Type.LONG, 3)
      .mapping(Type.DOUBLE, 4)
      .mapping(Type.STRING, 5)
      .mapping(Type.BYTES, 6)
      .build();

  public static final EnumMapping<DiskDurabilityEnum> DURABILITY_ENUM_MAPPING =
      EnumMappingBuilder.newEnumMappingBuilder(DiskDurabilityEnum.class)
          .mapping(DiskDurabilityEnum.EVENTUAL, 0)
          .mapping(DiskDurabilityEnum.EVERY_MUTATION, 1)
          .mapping(DiskDurabilityEnum.TIMED, 2)
          .build();

  public static final Struct DURABILITY_STRUCT = StructBuilder.newStructBuilder()
      .enm("diskDurabilityEnum", 10, DURABILITY_ENUM_MAPPING)
      .int64("millisDuration", 20)
      .build();

  public static final EnumMapping<PersistentStorageType> PERSISTENT_STORAGE_TYPE_ENUM_MAPPING =
      EnumMappingBuilder.newEnumMappingBuilder(PersistentStorageType.class)
          .mapping(PersistentStorageEngine.FRS, PersistentStorageEngine.FRS.getPermanentId())
          .mapping(PersistentStorageEngine.HYBRID, PersistentStorageEngine.HYBRID.getPermanentId())
          .build();

  public static final Struct KEY_STRUCT = StructBuilder.newStructBuilder()
      .enm("keyType", 10, TYPE_ENUM_MAPPING)
      .bool("bool", 20)
      .chr("char", 30)
      .int32("int", 40)
      .int64("long", 50)
      .fp64("double", 60)
      .string("string", 70)
      .build();

  public static final Struct CELL_DEFINITION_STRUCT = StructBuilder.newStructBuilder()
      .string("name", 10)
      .enm("type", 20, TYPE_ENUM_MAPPING)
      .build();

  public static final Struct CELL_STRUCT = StructBuilder.newStructBuilder()
      .string("name", 10)
      .enm("type", 20, TYPE_ENUM_MAPPING)
      .bool("bool", 30)
      .chr("char", 40)
      .int32("int", 50)
      .int64("long", 60)
      .fp64("double", 70)
      .string("string", 80)
      .byteBuffer("bytes", 90)
      .build();

  public static final Struct CELL_VALUE_STRUCT = StructBuilder.newStructBuilder()
          .enm("type", 10, TYPE_ENUM_MAPPING)
          .bool("bool", 20)
          .chr("char", 30)
          .int32("int", 40)
          .int64("long", 50)
          .fp64("double", 60)
          .string("string", 70)
          .byteBuffer("bytes", 80)
          .build();

  public static void encodeKey(StructEncoder<?> structEncoder, Comparable<?> key) {
    @SuppressWarnings("rawtypes")
    Type<? extends Comparable> keyType = Type.forJdkType(key.getClass());
    structEncoder.enm("keyType", keyType);
    typedEncode(structEncoder, keyType, key, false);
  }

  public static void encodeDefinition(PrimitiveEncodingSupport<?> encodingSupport, CellDefinition<?> definition) {
    encodingSupport.string("name", definition.name());
    encodingSupport.enm("type", definition.type());
  }

  public static void encodeCell(PrimitiveEncodingSupport<?> encodingSupport, Cell<?> cell) {
    if(cell == null) {
      throw new NullPointerException("Cells collection contains null entries");
    }
    String name = cell.definition().name();
    Type<?> type = cell.definition().type();
    Object value = cell.value();
    encodingSupport.string("name", name);
    encodingSupport.enm("type", type);
    typedEncode(encodingSupport, type, value, true);
  }

  public static void encodeCellValue(PrimitiveEncodingSupport<?> encodingSupport, Object value) {
    Type<?> type = Type.forJdkType(value.getClass());
    encodingSupport.enm("type", type);
    typedEncode(encodingSupport, type, value, true);
  }

  public static void encodeFunctionCellValue(StructEncoder<?> structEncoder, Intrinsic intrinsic) {
    structEncoder.enm("functionClass", intrinsic.getClass());

    structEncoder.struct("definition", ((CellValue<?>) intrinsic).getCellDefinition(), StoreStructures::encodeDefinition);

    Object defaultValue = ((CellValue<?>) intrinsic).getDefaultValue();
    if (defaultValue != null) {
      encodeCellValue(structEncoder.struct("defaultValue"), defaultValue);
    }
  }

  public static void encodeUUID(StructEncoder<?> structEncoder, UUID uuid) {
    structEncoder.int64("msb", uuid.getMostSignificantBits());
    structEncoder.int64("lsb", uuid.getLeastSignificantBits());
  }

  public static UUID decodeUUID(StructDecoder<?> structDecoder) {
    long msb = structDecoder.int64("msb");
    long lsb = structDecoder.int64("lsb");
    return new UUID(msb, lsb);
  }

  public static void encodeConstant(PrimitiveEncodingSupport<?> encodingSupport, Object value) {
    Type<?> type = Type.forJdkType(value.getClass());
    encodingSupport.enm("type", type);
    typedEncode(encodingSupport, type, value, true);
  }

  private static void typedEncode(PrimitiveEncodingSupport<?> encodingSupport, Type<?> type, Object value, boolean acceptBytes) {
    if (type.equals(Type.BOOL)) {
      encodingSupport.bool("bool", (Boolean) value);
    } else if (type.equals(Type.CHAR)) {
      encodingSupport.chr("char", (Character) value);
    } else if (type.equals(Type.INT)) {
      encodingSupport.int32("int", (Integer) value);
    } else if (type.equals(Type.LONG)) {
      encodingSupport.int64("long", (Long) value);
    } else if (type.equals(Type.DOUBLE)) {
      encodingSupport.fp64("double", (Double) value);
    } else if (type.equals(Type.STRING)) {
      encodingSupport.string("string", (String) value);
    } else if (acceptBytes && type.equals(Type.BYTES)) {
      encodingSupport.byteBuffer("bytes", ByteBuffer.wrap((byte[]) value));
    } else {
      throw new RuntimeException("Unknown type : " + type, null);
    }
  }

  public static CellDefinition<?> decodeDefinition(PrimitiveDecodingSupport definitionStructDecoder) {
    String name = definitionStructDecoder.string("name");
    Enm<Type<?>> type = definitionStructDecoder.enm("type");
    return CellDefinition.define(name, type.get());
  }

  public static <T> Cell<T> decodeCell(PrimitiveDecodingSupport cellStructDecoder) {
    String cellName = cellStructDecoder.string("name");
    Enm<Type<T>> cellTypeEnm = cellStructDecoder.enm("type");
    Type<T> cellType = cellTypeEnm.get();
    T cellValue = typedDecode(cellStructDecoder, cellType, true);

    CellDefinition<T> cellDefinition = CellDefinition.define(cellName, cellType);
    return cellDefinition.newCell(cellValue);
  }

  public static Intrinsic decodeConstant(StructDecoder<? extends StructDecoder<?>> constantStructDecoder) {
    return new Constant<>(decodeCellValue(constantStructDecoder));
  }

  public static Object decodeCellValue(StructDecoder<? extends StructDecoder<?>> constantStructDecoder) {
    Enm<Type<?>> type = constantStructDecoder.enm("type");
    return typedDecode(constantStructDecoder, type.get(), true);
  }

  public static Intrinsic decodeFunctionCellValue(StructDecoder<?> structDecoder) {
    Class<?> functionClass = (Class) structDecoder.enm("functionClass").get();

    StructDecoder<? extends StructDecoder<?>> definition = structDecoder.struct("definition");
    CellDefinition<?> cellDefinition = decodeDefinition(definition);

    StructDecoder<? extends StructDecoder<?>> defaultValueStruct = structDecoder.struct("defaultValue");
    Object defaultValue = null;
    if (defaultValueStruct != null) {
      Type<?> type = (Type) defaultValueStruct.enm("type").get();
      defaultValue = typedDecode(defaultValueStruct, type, true);
    }

    Constructor<?>[] ctors = functionClass.getConstructors();
    List<Exception> exceptions = new ArrayList<>();
    for (Constructor<?> ctor : ctors) {
      try {
        return (Intrinsic) ctor.newInstance(cellDefinition, defaultValue);
      } catch (Exception e) {
        exceptions.add(e);
      }
    }

    RuntimeException runtimeException = new RuntimeException("Cannot decode cell value function from (" + cellDefinition + ", " + defaultValue + ")");
    for (Exception exception : exceptions) {
      runtimeException.addSuppressed(exception);
    }
    throw runtimeException;
  }

  public static List<Cell<?>> decodeCells(StructArrayDecoder<?> cellsStructDecoder) {
    if (cellsStructDecoder == null) {
      return null;
    }

    List<Cell<?>> result = new ArrayList<>();

    for (int i = 0; i < cellsStructDecoder.length(); i++) {
      result.add(decodeCell(cellsStructDecoder.next()));
    }

    cellsStructDecoder.end();
    return result;
  }

  public static <K extends Comparable<K>> K decodeKey(StructDecoder<?> structDecoder) {
    Type<K> keyType = structDecoder.<Type<K>>enm("keyType").get();
    return typedDecode(structDecoder, keyType, false);
  }

  @SuppressWarnings("unchecked")
  private static <T> T typedDecode(PrimitiveDecodingSupport decoder, Type<T> type, boolean acceptBytes) {
    switch (type.asEnum()) {
      case BOOL:
        return (T) decoder.bool("bool");
      case CHAR:
        return (T) decoder.chr("char");
      case INT:
        return (T) decoder.int32("int");
      case LONG:
        return (T) decoder.int64("long");
      case DOUBLE:
        return (T) decoder.fp64("double");
      case STRING:
        return (T) decoder.string("string");
      case BYTES:
        if (acceptBytes) {
          ByteBuffer buffer = decoder.byteBuffer("bytes");
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          return (T) bytes;
        }
        throw new RuntimeException(type + " is not acceptable.");
      default:
        throw new RuntimeException("Unknown type : " + type);
    }
  }

  public static void encodeDiskDurability(StructEncoder<?> structEncoder, DiskDurability durability) {
    structEncoder.enm("diskDurabilityEnum", durability.getDurabilityEnum());
    if (durability instanceof BaseDiskDurability.Timed) {
      structEncoder.int64("millisDuration", ((BaseDiskDurability.Timed) durability).getMillisDuration());
    }
  }

  public static DiskDurability decodeDiskDurability(StructDecoder<?> diskDurabilityStructDecoder) {
    if (diskDurabilityStructDecoder == null) {
      return null;
    }
    DiskDurabilityEnum diskDurabilityEnum = diskDurabilityStructDecoder.
        <DiskDurabilityEnum>enm("diskDurabilityEnum").get();
    switch (diskDurabilityEnum) {
      case TIMED:
        return BaseDiskDurability.timed(diskDurabilityStructDecoder.int64("millisDuration"), TimeUnit.MILLISECONDS);
      case EVENTUAL:
        return BaseDiskDurability.OS_DETERMINED;
      case EVERY_MUTATION:
        return BaseDiskDurability.ALWAYS;
      default:
        return BaseDiskDurability.OS_DETERMINED;
    }
  }
}
