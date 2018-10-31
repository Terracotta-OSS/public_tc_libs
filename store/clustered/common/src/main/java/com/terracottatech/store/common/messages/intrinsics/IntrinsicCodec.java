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

import com.terracottatech.store.Cell;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.StoreStructures;
import com.terracottatech.store.common.messages.stream.NonPortableTransform;
import com.terracottatech.store.common.messages.stream.inline.WaypointMarker;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.function.BuildableStringFunction;
import com.terracottatech.store.intrinsics.CellDefinitionExists;
import com.terracottatech.store.intrinsics.IdentityFunction;
import com.terracottatech.store.intrinsics.InputMapper;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicBuildableStringFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToDoubleFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToIntFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToLongFunction;
import com.terracottatech.store.intrinsics.IntrinsicCollector;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicToDoubleFunction;
import com.terracottatech.store.intrinsics.IntrinsicToIntFunction;
import com.terracottatech.store.intrinsics.IntrinsicToLongFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;
import com.terracottatech.store.intrinsics.OutputMapper;
import com.terracottatech.store.intrinsics.impl.Add;
import com.terracottatech.store.intrinsics.impl.AllOfOperation;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import com.terracottatech.store.intrinsics.impl.BinaryBoolean;
import com.terracottatech.store.intrinsics.impl.CellExtractor;
import com.terracottatech.store.intrinsics.impl.CellValue;
import com.terracottatech.store.intrinsics.impl.ComparableComparator;
import com.terracottatech.store.intrinsics.impl.ComparisonType;
import com.terracottatech.store.intrinsics.impl.ConcurrentGroupingCollector;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.intrinsics.impl.CountingCollector;
import com.terracottatech.store.intrinsics.impl.DefaultGroupingCollector;
import com.terracottatech.store.intrinsics.impl.Divide;
import com.terracottatech.store.intrinsics.impl.FilteringCollector;
import com.terracottatech.store.intrinsics.impl.GatedComparison;
import com.terracottatech.store.intrinsics.impl.InstallOperation;
import com.terracottatech.store.intrinsics.impl.IntrinsicLogger;
import com.terracottatech.store.intrinsics.impl.MappingCollector;
import com.terracottatech.store.intrinsics.impl.Multiply;
import com.terracottatech.store.intrinsics.impl.Negation;
import com.terracottatech.store.intrinsics.impl.NonGatedComparison;
import com.terracottatech.store.intrinsics.impl.PartitioningCollector;
import com.terracottatech.store.intrinsics.impl.PredicateFunctionAdapter;
import com.terracottatech.store.intrinsics.impl.RecordKey;
import com.terracottatech.store.intrinsics.impl.RecordSameness;
import com.terracottatech.store.intrinsics.impl.RemoveOperation;
import com.terracottatech.store.intrinsics.impl.ReverseComparableComparator;
import com.terracottatech.store.intrinsics.impl.StringCellExtractor;
import com.terracottatech.store.intrinsics.impl.SummarizingDoubleCollector;
import com.terracottatech.store.intrinsics.impl.SummarizingIntCollector;
import com.terracottatech.store.intrinsics.impl.SummarizingLongCollector;
import com.terracottatech.store.intrinsics.impl.ToDoubleFunctionAdapter;
import com.terracottatech.store.intrinsics.impl.ToIntFunctionAdapter;
import com.terracottatech.store.intrinsics.impl.ToLongFunctionAdapter;
import com.terracottatech.store.intrinsics.impl.TupleIntrinsic;
import com.terracottatech.store.intrinsics.impl.WriteOperation;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.EnumMappingBuilder;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static com.terracottatech.store.common.messages.StoreStructures.CELL_DEFINITION_STRUCT;
import static com.terracottatech.store.common.messages.StoreStructures.CELL_STRUCT;
import static com.terracottatech.store.common.messages.StoreStructures.CELL_VALUE_STRUCT;
import static com.terracottatech.store.common.messages.StoreStructures.decodeCells;
import static com.terracottatech.store.common.messages.StoreStructures.decodeConstant;
import static com.terracottatech.store.common.messages.StoreStructures.decodeDefinition;
import static com.terracottatech.store.common.messages.StoreStructures.decodeFunctionCellValue;
import static com.terracottatech.store.common.messages.StoreStructures.decodeKey;
import static com.terracottatech.store.common.messages.StoreStructures.encodeKey;
import static com.terracottatech.store.intrinsics.IntrinsicType.ADD_DOUBLE;
import static com.terracottatech.store.intrinsics.IntrinsicType.ADD_INT;
import static com.terracottatech.store.intrinsics.IntrinsicType.ADD_LONG;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_FILTERING;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_GROUPING;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_GROUPING_CONCURRENT;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_MAPPING;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_PARTITIONING;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_SUMMARIZING_DOUBLE;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_SUMMARIZING_INT;
import static com.terracottatech.store.intrinsics.IntrinsicType.COLLECTOR_SUMMARIZING_LONG;
import static com.terracottatech.store.intrinsics.IntrinsicType.COMPARATOR;
import static com.terracottatech.store.intrinsics.IntrinsicType.CONSUMER_LOG;
import static com.terracottatech.store.intrinsics.IntrinsicType.COUNTING_COLLECTOR;
import static com.terracottatech.store.intrinsics.IntrinsicType.DIVIDE_DOUBLE;
import static com.terracottatech.store.intrinsics.IntrinsicType.DIVIDE_INT;
import static com.terracottatech.store.intrinsics.IntrinsicType.DIVIDE_LONG;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_CELL;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_CELL_VALUE;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_CONSTANT;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_PREDICATE_ADAPTER;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_RECORD_KEY;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_TO_DOUBLE_ADAPTER;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_TO_INT_ADAPTER;
import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_TO_LONG_ADAPTER;
import static com.terracottatech.store.intrinsics.IntrinsicType.IDENTITY_FUNCTION;
import static com.terracottatech.store.intrinsics.IntrinsicType.INPUT_MAPPER;
import static com.terracottatech.store.intrinsics.IntrinsicType.MULTIPLY_DOUBLE;
import static com.terracottatech.store.intrinsics.IntrinsicType.MULTIPLY_INT;
import static com.terracottatech.store.intrinsics.IntrinsicType.MULTIPLY_LONG;
import static com.terracottatech.store.intrinsics.IntrinsicType.NON_PORTABLE_TRANSFORM;
import static com.terracottatech.store.intrinsics.IntrinsicType.OPTIONAL_STRING_LENGTH;
import static com.terracottatech.store.intrinsics.IntrinsicType.OPTIONAL_STRING_STARTS_WITH;
import static com.terracottatech.store.intrinsics.IntrinsicType.OUTPUT_MAPPER;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_ALWAYS_TRUE;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_AND;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_CELL_DEFINITION_EXISTS;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_CONTRAST;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_EQUALS;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_GATED_CONTRAST;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_GATED_EQUALS;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_NEGATE;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_OR;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_RECORD_EQUALS;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_RECORD_SAME;
import static com.terracottatech.store.intrinsics.IntrinsicType.REVERSE_COMPARATOR;
import static com.terracottatech.store.intrinsics.IntrinsicType.STRING_LENGTH;
import static com.terracottatech.store.intrinsics.IntrinsicType.STRING_STARTS_WITH;
import static com.terracottatech.store.intrinsics.IntrinsicType.TUPLE_FIRST;
import static com.terracottatech.store.intrinsics.IntrinsicType.TUPLE_SECOND;
import static com.terracottatech.store.intrinsics.IntrinsicType.UPDATE_OPERATION_ALL_OF;
import static com.terracottatech.store.intrinsics.IntrinsicType.UPDATE_OPERATION_INSTALL;
import static com.terracottatech.store.intrinsics.IntrinsicType.UPDATE_OPERATION_REMOVE;
import static com.terracottatech.store.intrinsics.IntrinsicType.UPDATE_OPERATION_WRITE;
import static com.terracottatech.store.intrinsics.IntrinsicType.WAYPOINT;

@SuppressWarnings({"unchecked", "rawtypes"})
public class IntrinsicCodec {

  public static final Map<IntrinsicType, IntrinsicDescriptor> INTRINSIC_DESCRIPTORS = new EnumMap<>(IntrinsicType.class);

  private static final EnumMapping<ComparisonType> COMPARISON_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(ComparisonType.class)
      .mapping(ComparisonType.EQ, 0)
      .mapping(ComparisonType.NEQ, 1)
      .mapping(ComparisonType.GREATER_THAN, 2)
      .mapping(ComparisonType.GREATER_THAN_OR_EQUAL, 3)
      .mapping(ComparisonType.LESS_THAN, 4)
      .mapping(ComparisonType.LESS_THAN_OR_EQUAL, 5)
      .build();

  private static final EnumMapping<Class> FUNCTION_CLASS_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(Class.class)
      .mapping(CellValue.class, 0)
      .mapping(CellValue.ComparableCellValue.class, 1)
      .mapping(CellValue.IntCellValue.class, 2)
      .mapping(CellValue.LongCellValue.class, 3)
      .mapping(CellValue.DoubleCellValue.class, 4)
      .mapping(CellValue.StringCellValue.class, 5)
      .build();

  static {
    INTRINSIC_DESCRIPTORS.put(PREDICATE_RECORD_EQUALS, new IntrinsicDescriptor("predicateRecordEquals", 0,
            (intrinsicStruct) -> new DatasetStructBuilder(intrinsicStruct, null)
                    .key("key", 10)
                    .int64("msn", 20)
                    .cells("cells", 30)
                .build(),
            null, null // serialization and deserialization of the record is server and client-specific, there cannot be a default implementation
    ));
    INTRINSIC_DESCRIPTORS.put(PREDICATE_RECORD_SAME, new IntrinsicDescriptor("predicateRecordSame", 1,
            (intrinsicStruct) -> new DatasetStructBuilder(intrinsicStruct, null)
                    .key("key", 10)
                    .int64("msn", 20)
                .build(),
        (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
          RecordSameness<?> recordSameness = (RecordSameness<?>) intrinsic;
          encodeKey(intrinsicEncoder.struct("key"), recordSameness.getKey());
          intrinsicEncoder.int64("msn", recordSameness.getMSN());
        },
        (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
          Comparable key = decodeKey(intrinsicDecoder.struct("key"));
          Long msn = intrinsicDecoder.int64("msn");
          return new RecordSameness<>(msn, key);
        }
    ));
    INTRINSIC_DESCRIPTORS.put(PREDICATE_ALWAYS_TRUE, new IntrinsicDescriptor("predicateAlwaysTrue", 2,
            (intrinsicStruct) -> StructBuilder.newStructBuilder().build(),
            (intrinsicEncoder, intrinsic, intrinsicCodec) -> {},
            (intrinsicDecoder, intrinsicCodec) -> AlwaysTrue.alwaysTrue()
    ));
    INTRINSIC_DESCRIPTORS.put(PREDICATE_CELL_DEFINITION_EXISTS, new IntrinsicDescriptor("predicateCellDefinitionExists", 3,
            (intrinsicStruct) -> new DatasetStructBuilder(intrinsicStruct, null)
                    .cellDefinition("definition", 10)
                    .build(),
            (intrinsicEncoder, intrinsic, intrinsicCodec) -> {
              intrinsicEncoder.struct("definition", ((CellDefinitionExists) intrinsic).getCellDefinition(), StoreStructures::encodeDefinition);
            },
            (intrinsicDecoder, intrinsicCodec) -> {
                    CellDefinition<?> definition = decodeDefinition(intrinsicDecoder.struct("definition"));
                    return new CellDefinitionExists(definition);
            }
    ));
    INTRINSIC_DESCRIPTORS.put(UPDATE_OPERATION_INSTALL, new IntrinsicDescriptor("updateOperationInstall", 4,
            (intrinsicStruct) -> StructBuilder.newStructBuilder()
                    .structs("cells", 10, CELL_STRUCT)
                    .build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicEncoder.structs("cells", ((InstallOperation<?>) intrinsic).getCells(), StoreStructures::encodeCell);
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              Collection<Cell<?>> cells = decodeCells(intrinsicDecoder.structs("cells"));
              return new InstallOperation<>(cells);
            }
    ));
    INTRINSIC_DESCRIPTORS.put(UPDATE_OPERATION_WRITE, new IntrinsicDescriptor("updateOperationWrite", 5,
        (intrinsicStruct) -> StructBuilder.newStructBuilder()
            .struct("definition", 10, CELL_DEFINITION_STRUCT)
            .struct("value", 20, intrinsicStruct)
            .build(),
        (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
          intrinsicEncoder.struct("definition", ((WriteOperation<?,?>) intrinsic).definition(), StoreStructures::encodeDefinition);
          intrinsicEncoder.struct("value", ((WriteOperation<?,?>) intrinsic).getValue(), intrinsicCodec::encodeIntrinsic);
        },
        (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
          CellDefinition<?> definition = decodeDefinition(intrinsicDecoder.struct("definition"));
          IntrinsicFunction value = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("value"));
          return new WriteOperation(definition, value);
        }
    ));
    INTRINSIC_DESCRIPTORS.put(UPDATE_OPERATION_REMOVE, new IntrinsicDescriptor("updateOperationRemove", 6,
        (intrinsicStruct) -> StructBuilder.newStructBuilder()
            .struct("definition", 10, CELL_DEFINITION_STRUCT)
            .build(),
        (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
          intrinsicEncoder.struct("definition", ((RemoveOperation<?,?>) intrinsic).definition(), StoreStructures::encodeDefinition);
        },
        (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
          CellDefinition definition = decodeDefinition(intrinsicDecoder.struct("definition"));
          return new RemoveOperation(definition);
        }
    ));
    INTRINSIC_DESCRIPTORS.put(UPDATE_OPERATION_ALL_OF, new IntrinsicDescriptor("updateOperationAllOf", 7,
        (intrinsicStruct) -> StructBuilder.newStructBuilder()
            .structs("transforms", 10, intrinsicStruct)
            .build(),
        (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
          intrinsicEncoder.structs("transforms", ((AllOfOperation) intrinsic).getOperations(), intrinsicCodec::encodeIntrinsic);
        },
        (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
          List<Intrinsic> transforms = intrinsicCodec.decodeIntrinsics(intrinsicDecoder.structs("transforms"));
          return new AllOfOperation(transforms);
        }
    ));

    INTRINSIC_DESCRIPTORS.put(PREDICATE_AND, new IntrinsicDescriptor("predicateAnd", 8,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("left", 10, intrinsicStruct)
                    .struct("right", 20, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicEncoder.struct("left", ((BinaryBoolean.And<?>) intrinsic).getLeft(), intrinsicCodec::encodeIntrinsic);
              intrinsicEncoder.struct("right", ((BinaryBoolean.And<?>) intrinsic).getRight(), intrinsicCodec::encodeIntrinsic);
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicPredicate left = (IntrinsicPredicate) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("left"));
              IntrinsicPredicate right = (IntrinsicPredicate) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("right"));
              return new BinaryBoolean.And<>(left, right);
            }));

    INTRINSIC_DESCRIPTORS.put(PREDICATE_OR, new IntrinsicDescriptor("predicateOr", 9,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("left", 10, intrinsicStruct)
                    .struct("right", 20, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicEncoder.struct("left", ((BinaryBoolean.Or<?>) intrinsic).getLeft(), intrinsicCodec::encodeIntrinsic);
              intrinsicEncoder.struct("right", ((BinaryBoolean.Or<?>) intrinsic).getRight(), intrinsicCodec::encodeIntrinsic);
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicPredicate left = (IntrinsicPredicate) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("left"));
              IntrinsicPredicate right = (IntrinsicPredicate) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("right"));
              return new BinaryBoolean.Or<>(left, right);
            }));

    INTRINSIC_DESCRIPTORS.put(PREDICATE_NEGATE, new IntrinsicDescriptor("predicateNegate", 10,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("predicate", 10, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicEncoder.struct("predicate", ((Negation<?>) intrinsic).getExpression(), intrinsicCodec::encodeIntrinsic);
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicPredicate predicate = (IntrinsicPredicate) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("predicate"));
              return (Intrinsic) predicate.negate();
            }));

    INTRINSIC_DESCRIPTORS.put(PREDICATE_GATED_EQUALS, new IntrinsicDescriptor("gatedEquals", 11,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("left", 10, intrinsicStruct)
                    .struct("right", 20, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicEncoder.struct("left", ((GatedComparison<?, ?>) intrinsic).getLeft(), intrinsicCodec::encodeIntrinsic);
              intrinsicEncoder.struct("right", ((GatedComparison<?, ?>) intrinsic).getRight(), intrinsicCodec::encodeIntrinsic);
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicFunction left = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("left"));
              IntrinsicFunction right = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("right"));
              return new GatedComparison.Equals(left, right);
            }));

    INTRINSIC_DESCRIPTORS.put(PREDICATE_GATED_CONTRAST, new IntrinsicDescriptor("gatedContrast", 12,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("left", 10, intrinsicStruct)
                    .struct("right", 20, intrinsicStruct)
                    .enm("transform", 30, COMPARISON_MAPPING).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicEncoder.struct("left", ((GatedComparison<?, ?>) intrinsic).getLeft(), intrinsicCodec::encodeIntrinsic);
              intrinsicEncoder.struct("right", ((GatedComparison<?, ?>) intrinsic).getRight(), intrinsicCodec::encodeIntrinsic);
              intrinsicEncoder.enm("transform", ((GatedComparison.Contrast) intrinsic).getComparisonType());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicFunction left = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("left"));
              IntrinsicFunction right = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("right"));
              ComparisonType type = (ComparisonType) intrinsicDecoder.enm("transform").get();
              return new GatedComparison.Contrast<>(left, type, right);
            }));

    INTRINSIC_DESCRIPTORS.put(ADD_INT, new IntrinsicDescriptor("addInt", 13,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("function", 10, intrinsicStruct).int32("operand", 20).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
                    intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((Add.Int) intrinsic).getFunction());
                    intrinsicEncoder.int32("operand", ((Add.Int) intrinsic).getOperand());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicBuildableToIntFunction function = (IntrinsicBuildableToIntFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              Integer operand = intrinsicDecoder.int32("operand");
              return new Add.Int(function, operand);
            }));

    INTRINSIC_DESCRIPTORS.put(ADD_LONG, new IntrinsicDescriptor("addLong", 14,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("function", 10, intrinsicStruct).int64("operand", 20).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
                    intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((Add.Long) intrinsic).getFunction());
                    intrinsicEncoder.int64("operand", ((Add.Long) intrinsic).getOperand());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicBuildableToLongFunction function = (IntrinsicBuildableToLongFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              Long operand = intrinsicDecoder.int64("operand");
              return new Add.Long(function, operand);
            }));

    INTRINSIC_DESCRIPTORS.put(ADD_DOUBLE, new IntrinsicDescriptor("addDouble", 15,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("function", 10, intrinsicStruct).fp64("operand", 20).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
                    intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((Add.Double) intrinsic).getFunction());
                    intrinsicEncoder.fp64("operand", ((Add.Double) intrinsic).getOperand());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicBuildableToDoubleFunction function = (IntrinsicBuildableToDoubleFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              Double operand = intrinsicDecoder.fp64("operand");
              return new Add.Double<>(function, operand);
            }));

    INTRINSIC_DESCRIPTORS.put(COMPARATOR, new IntrinsicDescriptor("comparator", 16,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("function", 10, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
                    intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((ComparableComparator) intrinsic).getFunction()),
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
                    IntrinsicFunction function = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
                    return new ComparableComparator(function);
            }));

    INTRINSIC_DESCRIPTORS.put(REVERSE_COMPARATOR, new IntrinsicDescriptor("reverseComparator", 17,
          intrinsicStruct -> StructBuilder.newStructBuilder().struct("comparator", 10, intrinsicStruct).build(),
          (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
              intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("comparator"), ((ReverseComparableComparator) intrinsic).getOriginalComparator()),
          (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              ComparableComparator comparator = (ComparableComparator) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("comparator"));
              return new ReverseComparableComparator(comparator);
          }));

    INTRINSIC_DESCRIPTORS.put(FUNCTION_CONSTANT, new IntrinsicDescriptor("functionConstant", 18,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("value", 10, CELL_VALUE_STRUCT).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
                    intrinsicEncoder.struct("value", ((Constant<?, ?>) intrinsic).getValue(), StoreStructures::encodeConstant),
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) ->
                    decodeConstant(intrinsicDecoder.struct("value"))));

    INTRINSIC_DESCRIPTORS.put(FUNCTION_CELL, new IntrinsicDescriptor("functionCell", 19,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("definition", 10, CELL_DEFINITION_STRUCT).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
                    intrinsicEncoder.struct("definition", ((CellExtractor<?>) intrinsic).extracts(), StoreStructures::encodeDefinition),
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
                    CellDefinition<?> definition = decodeDefinition(intrinsicDecoder.struct("definition"));
                    return (Intrinsic) definition.value();
    }));

    INTRINSIC_DESCRIPTORS.put(FUNCTION_CELL_VALUE, new IntrinsicDescriptor("functionCellValue", 20,
        intrinsicStruct -> StructBuilder.newStructBuilder()
            .enm("functionClass", 10, FUNCTION_CLASS_MAPPING)
            .struct("definition", 20, CELL_DEFINITION_STRUCT)
            .struct("defaultValue", 30, CELL_VALUE_STRUCT).build(),
        (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> StoreStructures.encodeFunctionCellValue(intrinsicEncoder, intrinsic),
        (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> decodeFunctionCellValue(intrinsicDecoder)));


    INTRINSIC_DESCRIPTORS.put(FUNCTION_PREDICATE_ADAPTER, new IntrinsicDescriptor("functionPredicateAdapter", 21,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("delegate", 10, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
                    intrinsicEncoder.struct("delegate", ((PredicateFunctionAdapter<?>) intrinsic).getDelegate(), intrinsicCodec::encodeIntrinsic),
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              Intrinsic delegate = intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("delegate"));
              return new PredicateFunctionAdapter<>((IntrinsicPredicate<?>) delegate);
            }));

    INTRINSIC_DESCRIPTORS.put(FUNCTION_TO_INT_ADAPTER, new IntrinsicDescriptor("functionToIntAdapter", 22,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("delegate", 10, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
                    intrinsicEncoder.struct("delegate", ((ToIntFunctionAdapter<?>) intrinsic).getDelegate(), intrinsicCodec::encodeIntrinsic),
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicToIntFunction<?> delegate = (IntrinsicToIntFunction<?>) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("delegate"));
              return new ToIntFunctionAdapter<>(delegate);
    }));

    INTRINSIC_DESCRIPTORS.put(FUNCTION_TO_LONG_ADAPTER, new IntrinsicDescriptor("functionToLongAdapter", 23,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("delegate", 10, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
                    intrinsicEncoder.struct("delegate", ((ToLongFunctionAdapter<?>) intrinsic).getDelegate(), intrinsicCodec::encodeIntrinsic),
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicToLongFunction<?> delegate = (IntrinsicToLongFunction<?>) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("delegate"));
              return new ToLongFunctionAdapter<>(delegate);
    }));

    INTRINSIC_DESCRIPTORS.put(FUNCTION_TO_DOUBLE_ADAPTER, new IntrinsicDescriptor("functionToDoubleAdapter", 24,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("delegate", 10, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
                    intrinsicEncoder.struct("delegate", ((ToDoubleFunctionAdapter<?>) intrinsic).getDelegate(), intrinsicCodec::encodeIntrinsic),
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicToDoubleFunction<?> delegate = (IntrinsicToDoubleFunction<?>) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("delegate"));
              return new ToDoubleFunctionAdapter<>(delegate);
    }));

    INTRINSIC_DESCRIPTORS.put(COLLECTOR_SUMMARIZING_INT, new IntrinsicDescriptor("collectorSummarizingInt", 25,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("mapper", 10, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
                intrinsicEncoder.struct("mapper", ((SummarizingIntCollector) intrinsic).getMapper(), intrinsicCodec::encodeIntrinsic),
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicToIntFunction<?> mapping = (IntrinsicToIntFunction<?>) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("mapper"));
              return new SummarizingIntCollector(mapping);
    }));

    INTRINSIC_DESCRIPTORS.put(COLLECTOR_SUMMARIZING_LONG, new IntrinsicDescriptor("collectorSummarizingLong", 26,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("mapper", 10, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
                intrinsicEncoder.struct("mapper", ((SummarizingLongCollector) intrinsic).getMapper(), intrinsicCodec::encodeIntrinsic),
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicToLongFunction<?> mapping = (IntrinsicToLongFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("mapper"));
              return new SummarizingLongCollector(mapping);
    }));

    INTRINSIC_DESCRIPTORS.put(COLLECTOR_SUMMARIZING_DOUBLE, new IntrinsicDescriptor("collectorSummarizingDouble", 27,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("mapper", 10, intrinsicStruct).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
                intrinsicEncoder.struct("mapper", ((SummarizingDoubleCollector) intrinsic).getMapper(), intrinsicCodec::encodeIntrinsic),
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicToDoubleFunction<?> mapping = (IntrinsicToDoubleFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("mapper"));
              return new SummarizingDoubleCollector(mapping);
    }));

    INTRINSIC_DESCRIPTORS.put(FUNCTION_RECORD_KEY, new IntrinsicDescriptor("functionRecordKey", 28,
            intrinsicStruct -> StructBuilder.newStructBuilder().build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {},
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> new RecordKey<>()));

    INTRINSIC_DESCRIPTORS.put(WAYPOINT, new IntrinsicDescriptor("waypoint", 29,
        intrinsicStruct -> StructBuilder.newStructBuilder().int32("id", 10).build(),
        (intrinsicEncoder, intrinsic, intrinsicCodec) -> intrinsicEncoder.int32("id", ((WaypointMarker)intrinsic).getWaypointId()),
        (intrinsicDecoder, intrinsicCodec) -> new WaypointMarker<>(intrinsicDecoder.int32("id"))
    ));

    INTRINSIC_DESCRIPTORS.put(IDENTITY_FUNCTION, new IntrinsicDescriptor("identityFunction", 30,
        intrinsicStruct -> StructBuilder.newStructBuilder().build(),
        (intrinsicEncoder, intrinsic, intrinsicCodec) -> { },
        (intrinsicDecoder, intrinsicCodec) -> new IdentityFunction<>()
    ));

    INTRINSIC_DESCRIPTORS.put(INPUT_MAPPER, new IntrinsicDescriptor("inputMapper", 31,
        intrinsicStruct -> StructBuilder.newStructBuilder()
            .struct("function", 10, intrinsicStruct)
            .build(),
        (intrinsicEncoder, intrinsic, intrinsicCodec) ->
            intrinsicEncoder.struct("function", ((InputMapper<?, ?, ?>)intrinsic).getMapper(), intrinsicCodec::encodeIntrinsic),
        (intrinsicDecoder, intrinsicCodec) -> {
          IntrinsicFunction<?, ?> mapper = (IntrinsicFunction<?, ?>)intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
          return new InputMapper<>(mapper);
        }
    ));

    INTRINSIC_DESCRIPTORS.put(OUTPUT_MAPPER, new IntrinsicDescriptor("outputMapper", 32,
        intrinsicStruct -> StructBuilder.newStructBuilder()
            .struct("function", 10, intrinsicStruct)
            .build(),
        (intrinsicEncoder, intrinsic, intrinsicCodec) -> {
            intrinsicEncoder.struct("function", ((OutputMapper<?, ?, ?>)intrinsic).getMapper(), intrinsicCodec::encodeIntrinsic);
        },
        (intrinsicDecoder, intrinsicCodec) -> {
          IntrinsicFunction<?, ?> mapper = (IntrinsicFunction<?, ?>)intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
          return new OutputMapper<>(mapper);
        }
    ));

    INTRINSIC_DESCRIPTORS.put(NON_PORTABLE_TRANSFORM, new IntrinsicDescriptor("nonPortableTransform", 33,
        intrinsicStruct -> StructBuilder.newStructBuilder()
            .int32("waypointId", 10)
            .build(),
        (intrinsicEncoder, intrinsic, intrinsicCodec) -> {
          intrinsicEncoder.int32("waypointId", ((NonPortableTransform)intrinsic).getWaypointId());
        },
        (intrinsicDecoder, intrinsicCodec) -> new NonPortableTransform<>(intrinsicDecoder.int32("waypointId"))
    ));

    INTRINSIC_DESCRIPTORS.put(PREDICATE_EQUALS, new IntrinsicDescriptor("nonGatedEquals", 34,
        intrinsicStruct -> StructBuilder.newStructBuilder()
            .struct("left", 10, intrinsicStruct)
            .struct("right", 20, intrinsicStruct)
            .build(),
        (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
          intrinsicEncoder.struct("left", ((NonGatedComparison<?, ?>) intrinsic).getLeft(), intrinsicCodec::encodeIntrinsic);
          intrinsicEncoder.struct("right", ((NonGatedComparison<?, ?>) intrinsic).getRight(), intrinsicCodec::encodeIntrinsic);
        },
        (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
          IntrinsicFunction left = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("left"));
          IntrinsicFunction right = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("right"));
          return new NonGatedComparison.Equals<>(left, right);
        }));

    INTRINSIC_DESCRIPTORS.put(PREDICATE_CONTRAST, new IntrinsicDescriptor("nonGatedContrast", 35,
        intrinsicStruct -> StructBuilder.newStructBuilder()
            .struct("left", 10, intrinsicStruct)
            .struct("right", 20, intrinsicStruct)
            .enm("transform", 30, COMPARISON_MAPPING)
            .build(),
        (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
          intrinsicEncoder.struct("left", ((NonGatedComparison<?, ?>) intrinsic).getLeft(), intrinsicCodec::encodeIntrinsic);
          intrinsicEncoder.struct("right", ((NonGatedComparison<?, ?>) intrinsic).getRight(), intrinsicCodec::encodeIntrinsic);
          intrinsicEncoder.enm("transform", ((NonGatedComparison.Contrast) intrinsic).getComparisonType());
        },
        (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
          IntrinsicFunction left = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("left"));
          IntrinsicFunction right = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("right"));
          ComparisonType type = (ComparisonType) intrinsicDecoder.enm("transform").get();
          return new NonGatedComparison.Contrast<>(left, type, right);
        }));

    INTRINSIC_DESCRIPTORS.put(CONSUMER_LOG, new IntrinsicDescriptor("log", 36,
        intrinsicStruct -> StructBuilder.newStructBuilder()
            .string("message", 10)
            .structs("mappers", 20, intrinsicStruct)
            .build(),
        (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
          intrinsicEncoder.string("message", ((IntrinsicLogger) intrinsic).getMessage());
          intrinsicEncoder.structs("mappers", ((IntrinsicLogger) intrinsic).getMappers(), intrinsicCodec::encodeIntrinsic);
        },
        (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
          String logMessageFormat = intrinsicDecoder.string("message");
          List<Intrinsic> mappers = intrinsicCodec.decodeIntrinsics(intrinsicDecoder.structs("mappers"));
          return new IntrinsicLogger(logMessageFormat, mappers);
    }));

    INTRINSIC_DESCRIPTORS.put(TUPLE_FIRST, new IntrinsicDescriptor("tupleFirst", 37,
            intrinsicStruct -> StructBuilder.newStructBuilder().build(),
            (intrinsicEncoder, intrinsic, intrinsicCodec) -> { },
            (intrinsicDecoder, intrinsicCodec) -> TupleIntrinsic.first()
    ));

    INTRINSIC_DESCRIPTORS.put(TUPLE_SECOND, new IntrinsicDescriptor("tupleSecond", 38,
            intrinsicStruct -> StructBuilder.newStructBuilder().build(),
            (intrinsicEncoder, intrinsic, intrinsicCodec) -> { },
            (intrinsicDecoder, intrinsicCodec) -> TupleIntrinsic.second()
    ));

    INTRINSIC_DESCRIPTORS.put(COUNTING_COLLECTOR, new IntrinsicDescriptor("countingCollector", 39,
        intrinsicStruct -> StructBuilder.newStructBuilder().build(),
        (intrinsicEncoder, intrinsic, intrinsicCodec) -> { },
        ((intrinsicDecoder, intrinsicCodec) -> CountingCollector.counting())
    ));

    INTRINSIC_DESCRIPTORS.put(MULTIPLY_INT, new IntrinsicDescriptor("multiplyInt", 40,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("function", 10, intrinsicStruct).int32("operand", 20).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((Multiply.Int) intrinsic).getFunction());
              intrinsicEncoder.int32("operand", ((Multiply.Int) intrinsic).getOperand());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicBuildableToIntFunction function = (IntrinsicBuildableToIntFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              Integer operand = intrinsicDecoder.int32("operand");
              return new Multiply.Int(function, operand);
            }));

    INTRINSIC_DESCRIPTORS.put(MULTIPLY_LONG, new IntrinsicDescriptor("multiplyLong", 41,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("function", 10, intrinsicStruct).int64("operand", 20).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((Multiply.Long) intrinsic).getFunction());
              intrinsicEncoder.int64("operand", ((Multiply.Long) intrinsic).getOperand());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicBuildableToLongFunction function = (IntrinsicBuildableToLongFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              Long operand = intrinsicDecoder.int64("operand");
              return new Multiply.Long(function, operand);
            }));

    INTRINSIC_DESCRIPTORS.put(MULTIPLY_DOUBLE, new IntrinsicDescriptor("multiplyDouble", 42,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("function", 10, intrinsicStruct).fp64("operand", 20).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((Multiply.Double) intrinsic).getFunction());
              intrinsicEncoder.fp64("operand", ((Multiply.Double) intrinsic).getOperand());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicBuildableToDoubleFunction function = (IntrinsicBuildableToDoubleFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              Double operand = intrinsicDecoder.fp64("operand");
              return new Multiply.Double(function, operand);
            }));

    INTRINSIC_DESCRIPTORS.put(DIVIDE_INT, new IntrinsicDescriptor("divideInt", 43,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("function", 10, intrinsicStruct).int32("operand", 20).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((Divide.Int) intrinsic).getFunction());
              intrinsicEncoder.int32("operand", ((Divide.Int) intrinsic).getOperand());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicBuildableToIntFunction function = (IntrinsicBuildableToIntFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              Integer operand = intrinsicDecoder.int32("operand");
              return new Divide.Int(function, operand);
            }));

    INTRINSIC_DESCRIPTORS.put(DIVIDE_LONG, new IntrinsicDescriptor("divideLong", 44,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("function", 10, intrinsicStruct).int64("operand", 20).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((Divide.Long) intrinsic).getFunction());
              intrinsicEncoder.int64("operand", ((Divide.Long) intrinsic).getOperand());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicBuildableToLongFunction function = (IntrinsicBuildableToLongFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              Long operand = intrinsicDecoder.int64("operand");
              return new Divide.Long<>(function, operand);
            }));

    INTRINSIC_DESCRIPTORS.put(DIVIDE_DOUBLE, new IntrinsicDescriptor("divideDouble", 45,
            intrinsicStruct -> StructBuilder.newStructBuilder().struct("function", 10, intrinsicStruct).fp64("operand", 20).build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((Divide.Double) intrinsic).getFunction());
              intrinsicEncoder.fp64("operand", ((Divide.Double) intrinsic).getOperand());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicBuildableToDoubleFunction function = (IntrinsicBuildableToDoubleFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              Double operand = intrinsicDecoder.fp64("operand");
              return new Divide.Double<>(function, operand);
            }));
    INTRINSIC_DESCRIPTORS.put(STRING_LENGTH, new IntrinsicDescriptor("strlen", 46,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("function", 10, intrinsicStruct)
                    .build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((IntrinsicBuildableStringFunction.Length) intrinsic).getFunction());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              BuildableStringFunction function = (BuildableStringFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              return (Intrinsic) function.length();
            }));
    INTRINSIC_DESCRIPTORS.put(OPTIONAL_STRING_LENGTH, new IntrinsicDescriptor("opt_strlen", 47,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("function", 10, intrinsicStruct)
                    .build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), ((StringCellExtractor.Length) intrinsic).getFunction());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              StringCellExtractor function = (StringCellExtractor) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              return (Intrinsic) function.length();
            }));
    INTRINSIC_DESCRIPTORS.put(STRING_STARTS_WITH, new IntrinsicDescriptor("strStartsWith", 48,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("function", 10, intrinsicStruct)
                    .string("prefix", 20)
                    .build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicBuildableStringFunction.StartsWith startsWith = (IntrinsicBuildableStringFunction.StartsWith) intrinsic;
              intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), startsWith.getFunction());
              intrinsicEncoder.string("prefix", startsWith.getPrefix());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicBuildableStringFunction function = (IntrinsicBuildableStringFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              String prefix = intrinsicDecoder.string("prefix");
              return (Intrinsic) function.startsWith(prefix);
            }));
    INTRINSIC_DESCRIPTORS.put(OPTIONAL_STRING_STARTS_WITH, new IntrinsicDescriptor("opt_strStartsWith", 49,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("function", 10, intrinsicStruct)
                    .string("prefix", 20)
                    .build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) -> {
              StringCellExtractor.StartsWith startsWith = (StringCellExtractor.StartsWith) intrinsic;
              intrinsicCodec.encodeIntrinsic(intrinsicEncoder.struct("function"), startsWith.getFunction());
              intrinsicEncoder.string("prefix", startsWith.getPrefix());
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              StringCellExtractor function = (StringCellExtractor) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("function"));
              String prefix = intrinsicDecoder.string("prefix");
              return (Intrinsic) function.startsWith(prefix);
            }));
    INTRINSIC_DESCRIPTORS.put(COLLECTOR_GROUPING, new IntrinsicDescriptor("collectorGrouping", 50,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("classifier", 10, intrinsicStruct)
                    .struct("downstream", 20, intrinsicStruct)
                    .build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
            {
              DefaultGroupingCollector groupingCollector = (DefaultGroupingCollector) intrinsic;
              intrinsicEncoder.struct("classifier", groupingCollector.getFunction(), intrinsicCodec::encodeIntrinsic);
              intrinsicEncoder.struct("downstream", groupingCollector.getDownstream(), intrinsicCodec::encodeIntrinsic);
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicFunction<?, ?> mapping = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("classifier"));
              IntrinsicCollector<?, ?, ?> downstream = (IntrinsicCollector) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("downstream"));
              return new DefaultGroupingCollector(mapping, downstream);
            }));
    INTRINSIC_DESCRIPTORS.put(COLLECTOR_GROUPING_CONCURRENT, new IntrinsicDescriptor("collectorGroupingConcurrent", 51,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("classifier", 10, intrinsicStruct)
                    .struct("downstream", 20, intrinsicStruct)
                    .build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
            {
              ConcurrentGroupingCollector groupingCollector = (ConcurrentGroupingCollector) intrinsic;
              intrinsicEncoder.struct("classifier", groupingCollector.getFunction(), intrinsicCodec::encodeIntrinsic);
              intrinsicEncoder.struct("downstream", groupingCollector.getDownstream(), intrinsicCodec::encodeIntrinsic);
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicFunction<?, ?> mapping = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("classifier"));
              IntrinsicCollector<?, ?, ?> downstream = (IntrinsicCollector) intrinsicCodec.decodeIntrinsic(intrinsicDecoder.struct("downstream"));
              return new ConcurrentGroupingCollector(mapping, downstream);
            }));
    INTRINSIC_DESCRIPTORS.put(COLLECTOR_PARTITIONING, new IntrinsicDescriptor("collectorPartitioning", 52,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("predicate", 10, intrinsicStruct)
                    .struct("downstream", 20, intrinsicStruct)
                    .build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
            {
              PartitioningCollector groupingCollector = (PartitioningCollector) intrinsic;
              intrinsicEncoder.struct("predicate", groupingCollector.getFunction(), intrinsicCodec::encodeIntrinsic);
              intrinsicEncoder.struct("downstream", groupingCollector.getDownstream(), intrinsicCodec::encodeIntrinsic);
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicPredicate<?> mapping = (IntrinsicPredicate) intrinsicCodec.decodeIntrinsic(
                      intrinsicDecoder.struct("predicate"));
              IntrinsicCollector<?, ?, ?> downstream = (IntrinsicCollector) intrinsicCodec.decodeIntrinsic(
                      intrinsicDecoder.struct("downstream"));
              return new PartitioningCollector(mapping, downstream);
            }));
    INTRINSIC_DESCRIPTORS.put(COLLECTOR_MAPPING, new IntrinsicDescriptor("collectorMapping", 53,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("mapper", 10, intrinsicStruct)
                    .struct("downstream", 20, intrinsicStruct)
                    .build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
            {
              MappingCollector mappingCollector = (MappingCollector) intrinsic;
              intrinsicEncoder.struct("mapper", mappingCollector.getFunction(), intrinsicCodec::encodeIntrinsic);
              intrinsicEncoder.struct("downstream", mappingCollector.getDownstream(), intrinsicCodec::encodeIntrinsic);
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicFunction<?, ?> mapper = (IntrinsicFunction) intrinsicCodec.decodeIntrinsic(
                      intrinsicDecoder.struct("mapper"));
              IntrinsicCollector<?, ?, ?> downstream = (IntrinsicCollector) intrinsicCodec.decodeIntrinsic(
                      intrinsicDecoder.struct("downstream"));
              return new MappingCollector(mapper, downstream);
            }));
    INTRINSIC_DESCRIPTORS.put(COLLECTOR_FILTERING, new IntrinsicDescriptor("collectorFiltering", 54,
            intrinsicStruct -> StructBuilder.newStructBuilder()
                    .struct("predicate", 10, intrinsicStruct)
                    .struct("downstream", 20, intrinsicStruct)
                    .build(),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
            {
              FilteringCollector filteringCollector = (FilteringCollector) intrinsic;
              intrinsicEncoder.struct("predicate", filteringCollector.getFunction(), intrinsicCodec::encodeIntrinsic);
              intrinsicEncoder.struct("downstream", filteringCollector.getDownstream(), intrinsicCodec::encodeIntrinsic);
            },
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) -> {
              IntrinsicPredicate<?> predicate = (IntrinsicPredicate) intrinsicCodec.decodeIntrinsic(
                      intrinsicDecoder.struct("predicate"));
              IntrinsicCollector<?, ?, ?> downstream = (IntrinsicCollector) intrinsicCodec.decodeIntrinsic(
                      intrinsicDecoder.struct("downstream"));
              return new FilteringCollector(predicate, downstream);
            }));
  }

  private final Struct intrinsicStruct;
  private final EnumMap<IntrinsicType, IntrinsicDescriptor> intrinsicDescriptorMap;

  public IntrinsicCodec(Map<IntrinsicType, IntrinsicDescriptor> overrides) {
    intrinsicDescriptorMap = new EnumMap<>(INTRINSIC_DESCRIPTORS);
    intrinsicDescriptorMap.putAll(overrides);

    EnumMappingBuilder<IntrinsicType> intrinsicTypeEnumMappingBuilder = EnumMappingBuilder.newEnumMappingBuilder(IntrinsicType.class);
    for (Map.Entry<IntrinsicType, IntrinsicDescriptor> entry : intrinsicDescriptorMap.entrySet()) {
      IntrinsicType intrinsicType = entry.getKey();
      IntrinsicDescriptor intrinsicDescriptor = entry.getValue();

      intrinsicTypeEnumMappingBuilder.mapping(intrinsicType, intrinsicDescriptor.getTypeId());
    }
    EnumMapping<IntrinsicType> intrinsicTypeEnumMapping = intrinsicTypeEnumMappingBuilder.build();

    StructBuilder intrinsicStructBuilder = StructBuilder.newStructBuilder();
    Struct intrinsicStructAlias = intrinsicStructBuilder.alias();
    intrinsicStructBuilder.enm("intrinsic", 10, intrinsicTypeEnumMapping);
    for (Map.Entry<IntrinsicType, IntrinsicDescriptor> entry : intrinsicDescriptorMap.entrySet()) {
      IntrinsicType intrinsicType = entry.getKey();
      IntrinsicDescriptor intrinsicDescriptor = entry.getValue();
      try {
        intrinsicStructBuilder.struct(intrinsicDescriptor.getFieldName(),
            20 + intrinsicTypeEnumMapping.toInt(intrinsicType),
            intrinsicDescriptor.getStruct(intrinsicStructAlias));
      } catch (IllegalArgumentException e) {
        throw new AssertionError("Item order in IntrinsicType and INTRINSIC_DESCRIPTORS must match:" +
            " processing " + intrinsicType, e);
      }
    }
    intrinsicStruct = intrinsicStructBuilder.build();


    for (Map.Entry<IntrinsicType, IntrinsicDescriptor> entry : intrinsicDescriptorMap.entrySet()) {
      IntrinsicType intrinsicType = entry.getKey();
      IntrinsicDescriptor intrinsicDescriptor = entry.getValue();

      if (!intrinsicDescriptor.hasDecoder()) {
        throw new AssertionError("Intrinsic " + intrinsicType + " has no decoder");
      }
      if (!intrinsicDescriptor.hasEncoder()) {
        throw new AssertionError("Intrinsic " + intrinsicType + " has no encoder");
      }
    }
  }

  public Struct intrinsicStruct() {
    return intrinsicStruct;
  }


  public void encodeIntrinsic(StructEncoder intrinsicEncoder, Intrinsic intrinsic) {
    if (intrinsic == null) {
      return;
    }

    intrinsicEncoder.enm("intrinsic", intrinsic.getIntrinsicType());
    IntrinsicDescriptor intrinsicDescriptor = intrinsicDescriptorMap.get(intrinsic.getIntrinsicType());
    if (intrinsicDescriptor == null) {
      throw new RuntimeException("Unhandled intrinsic type : " + intrinsic.getIntrinsicType());
    }
    StructEncoder intrinsicSpecificsEncoder = intrinsicEncoder.struct(intrinsicDescriptor.getFieldName());
    intrinsicDescriptor.encode(intrinsicSpecificsEncoder, intrinsic, this);
    intrinsicSpecificsEncoder.end();
  }

  public List<Intrinsic> decodeIntrinsics(StructArrayDecoder<?> intrinsicsDecoder) {
    List<Intrinsic> result = new ArrayList<>();
    for (int i = 0; i < intrinsicsDecoder.length(); i++) {
      StructDecoder<? extends StructArrayDecoder<?>> next = intrinsicsDecoder.next();
      Intrinsic intrinsic = decodeIntrinsic(next);
      result.add(intrinsic);
    }
    intrinsicsDecoder.end();
    return result;
  }

  public Intrinsic decodeIntrinsic(StructDecoder intrinsicDecoder) {
    try {
      Enm<IntrinsicType> intrinsicEnm = intrinsicDecoder.enm("intrinsic");
      if (!intrinsicEnm.isFound()) {
        return null;
      }

      IntrinsicDescriptor intrinsicDescriptor = intrinsicDescriptorMap.get(intrinsicEnm.get());
      if (intrinsicDescriptor == null) {
        throw new RuntimeException("Unhandled intrinsic type : " + intrinsicEnm.get());
      }
      StructDecoder intrinsicSpecificsDecoder = intrinsicDecoder.struct(intrinsicDescriptor.getFieldName());
      Intrinsic decoded = intrinsicDescriptor.decode(intrinsicSpecificsDecoder, this);
      intrinsicSpecificsDecoder.end();
      return decoded;
    } finally {
      intrinsicDecoder.end();
    }
  }

}
