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

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.intrinsics.impl.ComparisonType;

/**
 * Interface to encapsulate each index range for the given cell.
 *
 * @author RKAV
 */
public interface IndexedCellRange<V extends Comparable<V>> {

  /**
   * Cell definition of the "indexed"  {@code Cell}.
   *
   * @return cell definition of the indexed cell
   */
  CellDefinition<V> getCellDefinition();

  /**
   * Actual compare operation being performed.
   * <p>
   * If the compare operation is {@link ComparisonType#LESS_THAN}
   * or {@link ComparisonType#LESS_THAN_OR_EQUAL},
   * the traversal will be in the reverse direction from {@link #start()} and {@link #end()} will be always be {@code null},
   * in that case. On the other hand, if the compare operation is
   * {@link ComparisonType#GREATER_THAN} or
   * {@link ComparisonType#GREATER_THAN_OR_EQUAL},
   * the traversal will be in forward direction and {@code end()}
   * may or may not be {@code null}.
   *
   * @return compare operation
   */
  ComparisonType operation();

  /**
   * Start of the range for the index traversal operation
   *
   * @return starting point of index traversal.
   */
  V start();

  /**
   * Optional end of the range for the operation. if this is null, starting point {@link #start} defines the
   * start and the operation defines whether the traversal will be in the reverse or forward direction.
   * <p>
   * If end is not null, traversal will always be in the forward direction from {@code start} to
   * {@code end}. If end is null, traversal will continue either till the beginning or till the end of the index
   * depending on the direction of traversal.
   *
   * @return ending point of traversal
   */
  V end();
}
