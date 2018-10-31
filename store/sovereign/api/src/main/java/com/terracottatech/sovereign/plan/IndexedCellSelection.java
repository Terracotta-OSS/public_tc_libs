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
package com.terracottatech.sovereign.plan;

import com.terracottatech.sovereign.SovereignDataset;

import java.util.List;

/**
 * Obtains one or more index ranges that needs q
 * to be scanned for given indexed {@code Cells} to filter
 * records of the {@code SovereignDataset}.
 * <p>
 * This interface is part of the {@code StreamPlan} and is available only when indexed cells are used
 * as {@code Predicate}s for the stream obtained by a call to {@link SovereignDataset#records()}
 */
public interface IndexedCellSelection {

  /**
   * Gets all index ranges that will be scanned. Includes start and end ranges.
   * If the expression contains only conjunctions of the indexed cell, there
   * will be only one item in the list. If an indexed cell is used under disjunction,
   * there could be more than one item in the list, signifying that multiple ranges
   * may need to be scanned.
   *
   * @return list of index ranges scanned
   */
  List<? extends IndexedCellRange<?>> indexRanges();

  default int countIndexRanges() {
    return indexRanges().size();
  }
}
