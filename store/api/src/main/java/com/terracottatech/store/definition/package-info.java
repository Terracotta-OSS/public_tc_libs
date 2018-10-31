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

/**
 * Contains types representing both generic and type-specific cell definitions.
 * <p>
 *   Cell definition instances act as:
 *   <ul>
 *     <li>symbolic cell references for retrieving cell values from records</li>
 *     <li>cell instance factories: {@link com.terracottatech.store.definition.CellDefinition#newCell(java.lang.Object)}.</li>
 *     <li>cell value extraction and manipulation function factories: {@link com.terracottatech.store.definition.CellDefinition#value()} (and others).
 *     </li>
 *   </ul>
 * <p>
 *   Type specific cell definitions allow for the accessing of construction of cell value manipulation
 *   functions that are applicable only for that cell type (e.g {@code stringCellDefinition.value().startsWith("Prefix")}.
 */
package com.terracottatech.store.definition;
