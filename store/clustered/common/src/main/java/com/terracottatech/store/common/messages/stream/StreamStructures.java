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
package com.terracottatech.store.common.messages.stream;

import com.terracottatech.store.Type;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.RecordData;
import com.terracottatech.store.intrinsics.Intrinsic;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.terracottatech.store.common.messages.StoreStructures.TYPE_ENUM_MAPPING;

public class StreamStructures {

  public static void encodePipelineOperation(DatasetStructEncoder encoder, PipelineOperation pipelineOperation) {
    encoder.getUnderlying().struct("operation", pipelineOperation.getOperation(), StreamStructures::encodeOperation);

    List<Intrinsic> arguments = pipelineOperation.getArguments().stream().map(argument -> {
      if (!(argument instanceof Intrinsic)) {
        throw new IllegalArgumentException("Not an Intrinsic: " + argument);
      }
      return (Intrinsic)argument;
    }).collect(Collectors.toList());
    encoder.intrinsics("argumentList", arguments);
  }

  public static PipelineOperation decodePipelineOperation(DatasetStructDecoder decoder) {
    PipelineOperation.Operation operation = decodeOperation(decoder.getUnderlying().struct("operation"));

    List<Intrinsic> arguments = decoder.intrinsics("argumentList");
    return operation.newInstance(arguments.toArray());
  }

  /**
   * The enumeration mappings for {@link PipelineOperation.TerminalOperation}.
   *
   * @see #OPERATION_STRUCT
   * @see OperationClass
   */
  private static final EnumMapping<PipelineOperation.TerminalOperation> TERMINAL_OPERATION_ENUM_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(PipelineOperation.TerminalOperation.class)
      .mapping(PipelineOperation.TerminalOperation.ALL_MATCH, 0)
      .mapping(PipelineOperation.TerminalOperation.ANY_MATCH, 1)
      .mapping(PipelineOperation.TerminalOperation.AVERAGE, 2)
      .mapping(PipelineOperation.TerminalOperation.COLLECT_1, 3)
      .mapping(PipelineOperation.TerminalOperation.COLLECT_3, 4)
      .mapping(PipelineOperation.TerminalOperation.COUNT, 5)
      .mapping(PipelineOperation.TerminalOperation.DOUBLE_ALL_MATCH, 6)
      .mapping(PipelineOperation.TerminalOperation.DOUBLE_ANY_MATCH, 7)
      .mapping(PipelineOperation.TerminalOperation.DOUBLE_COLLECT, 8)
      .mapping(PipelineOperation.TerminalOperation.DOUBLE_FOR_EACH, 9)
      .mapping(PipelineOperation.TerminalOperation.DOUBLE_FOR_EACH_ORDERED, 10)
      .mapping(PipelineOperation.TerminalOperation.DOUBLE_NONE_MATCH, 11)
      .mapping(PipelineOperation.TerminalOperation.DOUBLE_REDUCE_1, 12)
      .mapping(PipelineOperation.TerminalOperation.DOUBLE_REDUCE_2, 13)
      .mapping(PipelineOperation.TerminalOperation.FIND_ANY, 14)
      .mapping(PipelineOperation.TerminalOperation.FIND_FIRST, 15)
      .mapping(PipelineOperation.TerminalOperation.FOR_EACH, 16)
      .mapping(PipelineOperation.TerminalOperation.FOR_EACH_ORDERED, 17)
      .mapping(PipelineOperation.TerminalOperation.INT_ALL_MATCH, 18)
      .mapping(PipelineOperation.TerminalOperation.INT_ANY_MATCH, 19)
      .mapping(PipelineOperation.TerminalOperation.INT_COLLECT, 20)
      .mapping(PipelineOperation.TerminalOperation.INT_FOR_EACH, 21)
      .mapping(PipelineOperation.TerminalOperation.INT_FOR_EACH_ORDERED, 22)
      .mapping(PipelineOperation.TerminalOperation.INT_NONE_MATCH, 23)
      .mapping(PipelineOperation.TerminalOperation.INT_REDUCE_1, 24)
      .mapping(PipelineOperation.TerminalOperation.INT_REDUCE_2, 25)
      .mapping(PipelineOperation.TerminalOperation.ITERATOR, 26)
      .mapping(PipelineOperation.TerminalOperation.LONG_ALL_MATCH, 27)
      .mapping(PipelineOperation.TerminalOperation.LONG_ANY_MATCH, 28)
      .mapping(PipelineOperation.TerminalOperation.LONG_COLLECT, 29)
      .mapping(PipelineOperation.TerminalOperation.LONG_FOR_EACH, 30)
      .mapping(PipelineOperation.TerminalOperation.LONG_FOR_EACH_ORDERED, 31)
      .mapping(PipelineOperation.TerminalOperation.LONG_NONE_MATCH, 32)
      .mapping(PipelineOperation.TerminalOperation.LONG_REDUCE_1, 33)
      .mapping(PipelineOperation.TerminalOperation.LONG_REDUCE_2, 34)
      .mapping(PipelineOperation.TerminalOperation.MAX_0, 35)
      .mapping(PipelineOperation.TerminalOperation.MAX_1, 36)
      .mapping(PipelineOperation.TerminalOperation.MIN_0, 37)
      .mapping(PipelineOperation.TerminalOperation.MIN_1, 38)
      .mapping(PipelineOperation.TerminalOperation.NONE_MATCH, 39)
      .mapping(PipelineOperation.TerminalOperation.REDUCE_1, 40)
      .mapping(PipelineOperation.TerminalOperation.REDUCE_2, 41)
      .mapping(PipelineOperation.TerminalOperation.REDUCE_3, 42)
      .mapping(PipelineOperation.TerminalOperation.SPLITERATOR, 43)
      .mapping(PipelineOperation.TerminalOperation.SUM, 44)
      .mapping(PipelineOperation.TerminalOperation.SUMMARY_STATISTICS, 45)
      .mapping(PipelineOperation.TerminalOperation.TO_ARRAY_0, 46)
      .mapping(PipelineOperation.TerminalOperation.TO_ARRAY_1, 47)
      .mapping(PipelineOperation.TerminalOperation.DELETE, 48)
      .mapping(PipelineOperation.TerminalOperation.MUTATE, 49)
      .build();

  /**
   * The enumeration mappings for {@link PipelineOperation.IntermediateOperation}.
   *
   * @see #OPERATION_STRUCT
   * @see OperationClass
   */
  private static final EnumMapping<PipelineOperation.IntermediateOperation> INTERMEDIATE_OPERATION_ENUM_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(PipelineOperation.IntermediateOperation.class)
      .mapping(PipelineOperation.IntermediateOperation.AS_DOUBLE_STREAM, 0)
      .mapping(PipelineOperation.IntermediateOperation.AS_LONG_STREAM, 1)
      .mapping(PipelineOperation.IntermediateOperation.BOXED, 2)
      .mapping(PipelineOperation.IntermediateOperation.DISTINCT, 3)
      .mapping(PipelineOperation.IntermediateOperation.DOUBLE_FILTER, 4)
      .mapping(PipelineOperation.IntermediateOperation.DOUBLE_FLAT_MAP, 5)
      .mapping(PipelineOperation.IntermediateOperation.DOUBLE_MAP, 6)
      .mapping(PipelineOperation.IntermediateOperation.DOUBLE_MAP_TO_INT, 7)
      .mapping(PipelineOperation.IntermediateOperation.DOUBLE_MAP_TO_LONG, 8)
      .mapping(PipelineOperation.IntermediateOperation.DOUBLE_MAP_TO_OBJ, 9)
      .mapping(PipelineOperation.IntermediateOperation.DOUBLE_PEEK, 10)
      .mapping(PipelineOperation.IntermediateOperation.FILTER, 11)
      .mapping(PipelineOperation.IntermediateOperation.FLAT_MAP, 12)
      .mapping(PipelineOperation.IntermediateOperation.FLAT_MAP_TO_DOUBLE, 13)
      .mapping(PipelineOperation.IntermediateOperation.FLAT_MAP_TO_INT, 14)
      .mapping(PipelineOperation.IntermediateOperation.FLAT_MAP_TO_LONG, 15)
      .mapping(PipelineOperation.IntermediateOperation.INT_FILTER, 16)
      .mapping(PipelineOperation.IntermediateOperation.INT_FLAT_MAP, 17)
      .mapping(PipelineOperation.IntermediateOperation.INT_MAP, 18)
      .mapping(PipelineOperation.IntermediateOperation.INT_MAP_TO_DOUBLE, 19)
      .mapping(PipelineOperation.IntermediateOperation.INT_MAP_TO_LONG, 20)
      .mapping(PipelineOperation.IntermediateOperation.INT_MAP_TO_OBJ, 21)
      .mapping(PipelineOperation.IntermediateOperation.INT_PEEK, 22)
      .mapping(PipelineOperation.IntermediateOperation.LIMIT, 23)
      .mapping(PipelineOperation.IntermediateOperation.LONG_FILTER, 24)
      .mapping(PipelineOperation.IntermediateOperation.LONG_FLAT_MAP, 25)
      .mapping(PipelineOperation.IntermediateOperation.LONG_MAP, 26)
      .mapping(PipelineOperation.IntermediateOperation.LONG_MAP_TO_DOUBLE, 27)
      .mapping(PipelineOperation.IntermediateOperation.LONG_MAP_TO_INT, 28)
      .mapping(PipelineOperation.IntermediateOperation.LONG_MAP_TO_OBJ, 29)
      .mapping(PipelineOperation.IntermediateOperation.LONG_PEEK, 30)
      .mapping(PipelineOperation.IntermediateOperation.MAP, 31)
      .mapping(PipelineOperation.IntermediateOperation.MAP_TO_DOUBLE, 32)
      .mapping(PipelineOperation.IntermediateOperation.MAP_TO_INT, 33)
      .mapping(PipelineOperation.IntermediateOperation.MAP_TO_LONG, 34)
      .mapping(PipelineOperation.IntermediateOperation.ON_CLOSE, 35)
      .mapping(PipelineOperation.IntermediateOperation.PARALLEL, 36)
      .mapping(PipelineOperation.IntermediateOperation.PEEK, 37)
      .mapping(PipelineOperation.IntermediateOperation.SELF_CLOSE, 38)
      .mapping(PipelineOperation.IntermediateOperation.SEQUENTIAL, 39)
      .mapping(PipelineOperation.IntermediateOperation.SKIP, 40)
      .mapping(PipelineOperation.IntermediateOperation.SORTED_0, 41)
      .mapping(PipelineOperation.IntermediateOperation.SORTED_1, 42)
      .mapping(PipelineOperation.IntermediateOperation.UNORDERED, 43)
      .mapping(PipelineOperation.IntermediateOperation.EXPLAIN, 44)
      .mapping(PipelineOperation.IntermediateOperation.DELETE_THEN, 45)
      .mapping(PipelineOperation.IntermediateOperation.MUTATE_THEN, 46)
      .mapping(PipelineOperation.IntermediateOperation.MUTATE_THEN_INTERNAL, 47)
      .build();

  public static final EnumMapping<ElementType> ELEMENT_TYPE_ENUM_MAPPING =
      EnumMappingBuilder.newEnumMappingBuilder(ElementType.class)
          .mapping(ElementType.RECORD, 0)
          .mapping(ElementType.DOUBLE, 1)
          .mapping(ElementType.INT, 2)
          .mapping(ElementType.LONG, 3)
          .mapping(ElementType.ELEMENT_VALUE, 4)
          .mapping(ElementType.TERMINAL, 5)
          .build();

  public static Struct elementStruct(DatasetStructBuilder datasetStructBuilder) {
    return datasetStructBuilder
        .enm("elementType", 20, StreamStructures.ELEMENT_TYPE_ENUM_MAPPING)
        .record("recordData", 30)                                                           // ElementType.RECORD
        .fp64("doubleValue", 40)                                                            // ElementType.DOUBLE
        .int32("intValue", 50)                                                              // ElementType.INT
        .int64("longValue", 60)                                                             // ElementType.LONG
        .getUnderlying().struct("elementValue", 70, ELEMENT_VALUE_STRUCT)                   // ElementType.ELEMENT_VALUE
        .build();
  }

  public static void encodeElement(DatasetStructEncoder encoder, Element element) {
    encoder.enm("elementType", element.getType());
    switch (element.getType()) {
      case RECORD:
        encoder.record("recordData", element.getRecordData());
        break;
      case DOUBLE:
        encoder.fp64("doubleValue", element.getDoubleValue());
        break;
      case INT:
        encoder.int32("intValue", element.getIntValue());
        break;
      case LONG:
        encoder.int64("longValue", element.getLongValue());
        break;
      case ELEMENT_VALUE:
        encoder.getUnderlying().struct("elementValue", element.getElementValue(), StreamStructures::encodeElementValue);
        break;
      default:
        throw new UnsupportedOperationException("ElementType " + element.getType() + " not yet supported");
    }
  }

  public static Element decodeElement(DatasetStructDecoder decoder) {
    Enm<ElementType> elementTypeEnm = decoder.enm("elementType");
    if (!elementTypeEnm.isFound()) {
      return null;
    }
    ElementType elementType = elementTypeEnm.get();

    switch (elementType) {
      case RECORD: {
        RecordData<?> recordData = decoder.record("recordData");
        return new Element(elementType, recordData);
      }
      case DOUBLE: {
        double value = decoder.fp64("doubleValue");
        return new Element(elementType, value);
      }
      case INT: {
        int value = decoder.int32("intValue");
        return new Element(elementType, value);
      }
      case LONG: {
        long value = decoder.int64("longValue");
        return new Element(elementType, value);
      }
      case ELEMENT_VALUE: {
        StructDecoder<?> elementValueDecoder = decoder.getUnderlying().struct("elementValue");
        ElementValue elementValue = StreamStructures.decodeElementValue(elementValueDecoder);
        return new Element(elementType, elementValue);
      }
      default:
        throw new UnsupportedOperationException("ElementType " + elementType + " not yet supported");
    }
  }

  /**
   * A selector between {@link PipelineOperation.TerminalOperation} and {@link PipelineOperation.IntermediateOperation}.
   */
  private enum OperationClass {
    TERMINAL(PipelineOperation.TerminalOperation.class, "terminalOperation"),
    INTERMEDIATE(PipelineOperation.IntermediateOperation.class, "intermediateOperation");

    private final Class<? extends Enum<?>> enumClass;
    private final String encodedFieldName;

    OperationClass(Class<? extends Enum<?>> enumClass, String encodedFieldName) {
      this.enumClass = enumClass;
      this.encodedFieldName = encodedFieldName;
    }

    public String getEncodedFieldName() {
      return encodedFieldName;
    }

    public static OperationClass fromOperation(PipelineOperation.Operation op) {
      for (OperationClass opClass : OperationClass.values()) {
        if (opClass.enumClass.isInstance(op)) {
          return opClass;
        }
      }
      throw new EnumConstantNotPresentException(OperationClass.class, op.getClass().getName());
    }
  }

  private static final EnumMapping<OperationClass> OPERATION_CLASS_ENUM_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(OperationClass.class)
      .mapping(OperationClass.TERMINAL, 0)
      .mapping(OperationClass.INTERMEDIATE, 1)
      .build();

  /**
   * Describes the structure representing an {@link PipelineOperation.Operation Operation}.
   */
  private static final Struct OPERATION_STRUCT = StructBuilder.newStructBuilder()
      .enm("operationClass", 10, OPERATION_CLASS_ENUM_MAPPING)
      .enm(OperationClass.TERMINAL.getEncodedFieldName(), 20, TERMINAL_OPERATION_ENUM_MAPPING)
      .enm(OperationClass.INTERMEDIATE.getEncodedFieldName(), 30, INTERMEDIATE_OPERATION_ENUM_MAPPING)
      .build();

  private static void encodeOperation(PrimitiveEncodingSupport<?> encoder, PipelineOperation.Operation operation) {
    OperationClass opClass = OperationClass.fromOperation(operation);
    encoder.enm("operationClass", opClass);
    encoder.enm(opClass.getEncodedFieldName(), operation);
  }

  private static PipelineOperation.Operation decodeOperation(PrimitiveDecodingSupport decoder) {
    Enm<OperationClass> operationClassEnm = decoder.enm("operationClass");
    OperationClass operationClass = operationClassEnm.get();
    Enm<PipelineOperation.Operation> operationEnm = decoder.enm(operationClass.getEncodedFieldName());
    return operationEnm.get();
  }

  public static Struct pipelineOperationStruct(DatasetStructBuilder builder) {
    DatasetStructBuilder opBuilder = builder.newBuilder();
    opBuilder.getUnderlying().struct("operation", 10, OPERATION_STRUCT);
    return opBuilder
        .intrinsics("argumentList", 20)
        .build();
  }

  public static void encodeElementValue(StructEncoder<?> encoder, ElementValue elementValue) {
    encoder.enm("type", elementValue.getValueType());
    if (elementValue.getData() != null) {
      encoder.struct("primitive", elementValue.getData(), StreamStructures::encodePrimitive);
    }
    if (elementValue.getContainees() != null) {
      encoder.structs("containees", elementValue.getContainees(), StreamStructures::encodeElementValue);
    }
  }

  public static ElementValue decodeElementValue(StructDecoder<?> decoder) {
    ElementValue.ValueType valueType = decoder.<ElementValue.ValueType>enm("type").get();

    Primitive data = null;
    StructDecoder<?> dataDecoder = decoder.struct("primitive");
    if (dataDecoder != null) {
      data = StreamStructures.decodePrimitive(dataDecoder);
    }

    List<ElementValue> containees = null;
    StructArrayDecoder<? extends StructDecoder<?>> containeesDecoder = decoder.structs("containees");

    if (containeesDecoder != null) {
      containees = new ArrayList<>();
      for (int i = 0; i < containeesDecoder.length(); i++) {
        StructDecoder<?> containeeDecoder = containeesDecoder.next();
        ElementValue containee = decodeElementValue(containeeDecoder);
        containees.add(containee);
      }
    }

    return new ElementValue(valueType, data, containees);
  }

  private static final Struct PRIMITIVE_STRUCT = StructBuilder.newStructBuilder()
      .enm("type", 10, TYPE_ENUM_MAPPING)
      .bool("bool", 20)
      .chr("char", 30)
      .int32("int", 40)
      .int64("long", 50)
      .fp64("double", 60)
      .string("string", 70)
      .byteBuffer("bytes", 80)
      .build();

  private static void encodePrimitive(StructEncoder<?> encoder, Primitive primitive) {
    Type<?> type = primitive.getType();
    encoder.enm("type", type);

    switch (type.asEnum()) {
      case BOOL:
        encoder.bool("bool", (Boolean) primitive.getValue());
        break;
      case CHAR:
        encoder.chr("char", (Character) primitive.getValue());
        break;
      case INT:
        encoder.int32("int", (Integer) primitive.getValue());
        break;
      case LONG:
        encoder.int64("long", (Long) primitive.getValue());
        break;
      case DOUBLE:
        encoder.fp64("double", (Double) primitive.getValue());
        break;
      case STRING:
        encoder.string("string", (String) primitive.getValue());
        break;
      case BYTES:
        encoder.byteBuffer("bytes", ByteBuffer.wrap((byte[]) primitive.getValue()));
        break;
      default:
        throw new RuntimeException("Unknown type : " + type);
    }
  }

  private static Primitive decodePrimitive(StructDecoder<?> decoder) {
    Type<?> type = decoder.<Type<?>>enm("type").get();
    Object value;

    switch (type.asEnum()) {
      case BOOL:
        value = decoder.bool("bool");
        break;
      case CHAR:
        value = decoder.chr("char");
        break;
      case INT:
        value = decoder.int32("int");
        break;
      case LONG:
        value = decoder.int64("long");
        break;
      case DOUBLE:
        value = decoder.fp64("double");
        break;
      case STRING:
        value = decoder.string("string");
        break;
      case BYTES:
        ByteBuffer buffer = decoder.byteBuffer("bytes");
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        value = bytes;
        break;
      default:
        throw new RuntimeException("Unknown type : " + type);
    }

    return new Primitive(value);
  }

  private static EnumMapping<ElementValue.ValueType> ELEMENT_VALUE_TYPE_ENUM_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(ElementValue.ValueType.class)
          .mapping(ElementValue.ValueType.CELL, 0)
          .mapping(ElementValue.ValueType.PRIMITIVE, 1)
          .mapping(ElementValue.ValueType.OPTIONAL_LONG, 2)
          .mapping(ElementValue.ValueType.OPTIONAL_INT, 3)
          .mapping(ElementValue.ValueType.OPTIONAL_DOUBLE, 4)
          .mapping(ElementValue.ValueType.OPTIONAL, 5)
          .mapping(ElementValue.ValueType.RECORD, 6)
          .mapping(ElementValue.ValueType.INT_SUMMARY_STATISTICS, 7)
          .mapping(ElementValue.ValueType.LONG_SUMMARY_STATISTICS, 8)
          .mapping(ElementValue.ValueType.DOUBLE_SUMMARY_STATISTICS, 9)
          .mapping(ElementValue.ValueType.RECORD_DATA_TUPLE, 10)
          .mapping(ElementValue.ValueType.MAP, 11)
          .mapping(ElementValue.ValueType.CONCURRENT_MAP, 12)
          .build();

  public static final Struct ELEMENT_VALUE_STRUCT;

  static {
    StructBuilder structBuilder = StructBuilder.newStructBuilder();
    Struct alias = structBuilder.alias();
    ELEMENT_VALUE_STRUCT = structBuilder
        .enm("type", 10, ELEMENT_VALUE_TYPE_ENUM_MAPPING)
        .struct("primitive", 20, PRIMITIVE_STRUCT)
        .structs("containees", 30, alias)
        .build();
  }

}
