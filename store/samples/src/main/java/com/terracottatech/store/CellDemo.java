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
package com.terracottatech.store;

import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.BytesCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;

import static com.terracottatech.store.Cell.cell;

/**
 * A sample of {@link CellDefinition} and {@link Cell} functions.
 */
public class CellDemo {

  // In the following statements, the typical field qualifiers (public static final) are omitted for brevity.
  // tag::stringDefExample[]
  StringCellDefinition NAME = CellDefinition.defineString("nameCell");    // <1>
  CellDefinition<String> ALT_NAME = CellDefinition.defineString("altNameCell");   // <2>
  CellDefinition<String> BASIC_NAME =
      CellDefinition.define("basicNameCell", Type.STRING);    // <3>
  // end::stringDefExample[]

  // In the following statements, the typical field qualifiers (public static final) are omitted for brevity.
  // tag::otherDefExample[]
  DoubleCellDefinition WEIGHT = CellDefinition.defineDouble("weightCell");
  CellDefinition<Double> ALT_WEIGHT = CellDefinition.defineDouble("altWeightCell");
  CellDefinition<Double> BASIC_WEIGHT =
      CellDefinition.define("basicWeightCell", Type.DOUBLE);

  LongCellDefinition COUNT = CellDefinition.defineLong("countCell");
  CellDefinition<Long> ALT_COUNT = CellDefinition.defineLong("altCountCell");
  CellDefinition<Long> BASIC_COUNT = CellDefinition.define("basicCountCell", Type.LONG);

  CharCellDefinition CATEGORY = CellDefinition.defineChar("categoryCell");
  CellDefinition<Character> ALT_CATEGORY = CellDefinition.defineChar("altCategoryCell");
  CellDefinition<Character> BASIC_CATEGORY =
      CellDefinition.define("basicCategory", Type.CHAR);

  IntCellDefinition AGE = CellDefinition.defineInt("ageCell");
  CellDefinition<Integer> ALT_AGE = CellDefinition.defineInt("altAgeCell");
  CellDefinition<Integer> BASIC_AGE = CellDefinition.define("basicAgeCell", Type.INT);

  BoolCellDefinition EMPLOYED = CellDefinition.defineBool("employedCell");
  CellDefinition<Boolean> ALT_EMPLOYED = CellDefinition.defineBool("altEmployedCell");
  CellDefinition<Boolean> BASIC_EMPLOYED =
      CellDefinition.define("basicEmployedCell", Type.BOOL);

  BytesCellDefinition VAULT = CellDefinition.defineBytes("vaultCell");
  CellDefinition<byte[]> ALT_VAULT = CellDefinition.defineBytes("altVaultCell");
  CellDefinition<byte[]> BASIC_VAULT = CellDefinition.define("basicVaultCell", Type.BYTES);
  // end::otherDefExample[]

  public void showCellCreation() {
    // tag::cellCreation[]
    Cell<String> nameCell = NAME.newCell("Alex");     // <1>
    Cell<String> mollyCell = nameCell.definition().newCell("Molly");    // <2>

    Cell<String> aliasCell = Cell.cell("aliasCell", "Tattoo");      // <3>
    CellSet cells = CellSet.of(     // <4>
        cell("inlineNameCell", "Alex"),
        cell("inlineWeightCell", 118.5D),
        cell("inlineCountCell", 2L),
        cell("inlineCategoryCell", '®'),
        cell("inlineAgeCell", 42),
        cell("inlineEmployedCell", true),
        cell("inlineVaultCell",
            new byte[] {(byte) 0xCA,(byte) 0xFE,(byte) 0xBA,(byte) 0xBE}));
    // end::cellCreation[]
  }

}
