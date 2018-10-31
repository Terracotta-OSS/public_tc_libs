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

package com.terracottatech.store.embedded.demo;

import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.BytesCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;


/**
 * Employee class containing all existing seven cell types used for its attribute definition.
 */
public final class Employee {

  public static final StringCellDefinition NAME = CellDefinition.defineString("name");
  public static final CharCellDefinition GENDER = CellDefinition.defineChar("gender");
  public static final LongCellDefinition TELEPHONE = CellDefinition.defineLong("telephone");
  public static final BoolCellDefinition CURRENT = CellDefinition.defineBool("current");
  public static final IntCellDefinition SSN = CellDefinition.defineInt("ssn");
  public static final DoubleCellDefinition SALARY = CellDefinition.defineDouble("salary");
  public static final BytesCellDefinition SIGNATURE = CellDefinition.defineBytes("signature");

  public static final IntCellDefinition BIRTH_DAY = CellDefinition.defineInt("birthDay");
  public static final IntCellDefinition BIRTH_MONTH = CellDefinition.defineInt("birthMonth");
  public static final IntCellDefinition BIRTH_YEAR = CellDefinition.defineInt("birthYear");

  public static final IntCellDefinition HOUSE_NUMBER = CellDefinition.defineInt("houseNumber");
  public static final StringCellDefinition  STREET = CellDefinition.defineString("streetAddress");
  public static final StringCellDefinition  CITY = CellDefinition.defineString("cityAddress");
  public static final StringCellDefinition COUNTRY = CellDefinition.defineString("country");

  public static final DoubleCellDefinition BONUS = CellDefinition.defineDouble("bonus");

  public static final LongCellDefinition CELL_NUMBER = CellDefinition.defineLong("cellNumber");

}
