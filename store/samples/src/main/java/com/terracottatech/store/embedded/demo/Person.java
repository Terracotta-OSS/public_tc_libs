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

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.BytesCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;

/**
 * Defines the {@link CellDefinition} instances used in the {@link CrudDemo} example.
 */
// The formatting in this class is influenced by the inclusion of fragments from this class in documentation
public final class Person {

  public static final StringCellDefinition FIRST_NAME = CellDefinition.defineString("firstName");

  public static final //tag::lastNameDefinition[]
  StringCellDefinition LAST_NAME = CellDefinition.defineString("lastName");
  //end::lastNameDefinition[]

  public static final IntCellDefinition BIRTH_YEAR = CellDefinition.defineInt("birthYear");

  public static final BoolCellDefinition RATED = CellDefinition.defineBool("rated");

  public static final //tag::nicenessDefinition[]
  DoubleCellDefinition NICENESS = CellDefinition.defineDouble("niceness");
  //end::nicenessDefinition[]

  public static final BytesCellDefinition PICTURE = CellDefinition.defineBytes("picture");

}
