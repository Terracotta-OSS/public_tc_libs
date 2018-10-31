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
package com.terracottatech.store.intrinsics.impl;

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToDoubleFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToDoubleFunctionTest;

import java.util.Collections;

/**
 * Basic tests for {@link CellValue.DoubleCellValue} functions.
 */
public class DoubleCellValueTest extends IntrinsicBuildableToDoubleFunctionTest<Record<?>> {

  static final DoubleCellDefinition DOUBLE_CELL = CellDefinition.defineDouble("doubleCell");

  @Override
  protected IntrinsicBuildableToDoubleFunction<Record<?>> getFunction() {
    return new CellValue.DoubleCellValue(DOUBLE_CELL, 0.0D);
  }

  @Override
  protected Record<String> valueSource() {
    return new TestRecord<>("record", Collections.singleton(DOUBLE_CELL.newCell(0.0D)));
  }
}