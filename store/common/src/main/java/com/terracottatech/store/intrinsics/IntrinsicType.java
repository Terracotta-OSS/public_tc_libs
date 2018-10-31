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

package com.terracottatech.store.intrinsics;

public enum IntrinsicType {

  PREDICATE_RECORD_EQUALS,
  PREDICATE_RECORD_SAME,
  PREDICATE_ALWAYS_TRUE,
  PREDICATE_CELL_DEFINITION_EXISTS,

  UPDATE_OPERATION_INSTALL,
  UPDATE_OPERATION_WRITE,
  UPDATE_OPERATION_REMOVE,
  UPDATE_OPERATION_ALL_OF,

  PREDICATE_AND,
  PREDICATE_OR,
  PREDICATE_NEGATE,

  PREDICATE_GATED_EQUALS,
  PREDICATE_GATED_CONTRAST,

  ADD_INT,
  ADD_LONG,
  ADD_DOUBLE,

  COMPARATOR,
  REVERSE_COMPARATOR,

  FUNCTION_CONSTANT,
  FUNCTION_CELL,
  FUNCTION_CELL_VALUE,

  FUNCTION_PREDICATE_ADAPTER,
  FUNCTION_TO_INT_ADAPTER,
  FUNCTION_TO_LONG_ADAPTER,
  FUNCTION_TO_DOUBLE_ADAPTER,

  COLLECTOR_SUMMARIZING_INT,
  COLLECTOR_SUMMARIZING_LONG,
  COLLECTOR_SUMMARIZING_DOUBLE,

  FUNCTION_RECORD_KEY,

  WAYPOINT,
  IDENTITY_FUNCTION,
  INPUT_MAPPER,
  OUTPUT_MAPPER,

  NON_PORTABLE_TRANSFORM,

  PREDICATE_EQUALS,
  PREDICATE_CONTRAST,

  CONSUMER_LOG,

  TUPLE_FIRST,
  TUPLE_SECOND,

  COUNTING_COLLECTOR,

  MULTIPLY_INT,
  MULTIPLY_LONG,
  MULTIPLY_DOUBLE,

  DIVIDE_INT,
  DIVIDE_LONG,
  DIVIDE_DOUBLE,

  STRING_LENGTH,
  OPTIONAL_STRING_LENGTH,
  STRING_STARTS_WITH,
  OPTIONAL_STRING_STARTS_WITH,

  COLLECTOR_GROUPING,
  COLLECTOR_GROUPING_CONCURRENT,
  COLLECTOR_PARTITIONING,
  COLLECTOR_MAPPING,
  COLLECTOR_FILTERING;
}
