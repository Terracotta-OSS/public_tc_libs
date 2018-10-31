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
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToIntFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToIntFunctionTest;

import java.util.Collections;

/**
 * Basic tests for {@link com.terracottatech.store.intrinsics.impl.CellValue.IntCellValue} functions.
 */
public class IntCellValueTest extends IntrinsicBuildableToIntFunctionTest<Record<?>> {

  static final IntCellDefinition INT_CELL = CellDefinition.defineInt("intCell");

  @Override
  protected IntrinsicBuildableToIntFunction<Record<?>> getFunction() {
    return new CellValue.IntCellValue(INT_CELL, 0);
  }

  @Override
  protected Record<String> valueSource() {
    return new TestRecord<>("record", Collections.singleton(INT_CELL.newCell(0)));
  }
}