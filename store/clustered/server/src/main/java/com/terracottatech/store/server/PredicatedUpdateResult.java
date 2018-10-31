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

package com.terracottatech.store.server;

import com.terracottatech.store.Cell;

public class PredicatedUpdateResult {
  private Long beforeMsn;
  private Iterable<Cell<?>> beforeCells;
  private Long afterMsn;
  private Iterable<Cell<?>> afterCells;

  PredicatedUpdateResult(Long beforeMsn, Iterable<Cell<?>> beforeCells, Long afterMsn, Iterable<Cell<?>> afterCells) {
    this.beforeMsn = beforeMsn;
    this.beforeCells = beforeCells;
    this.afterMsn = afterMsn;
    this.afterCells = afterCells;
  }

  boolean applied() {
    return beforeMsn != null;
  }

  public Long getBeforeMsn() {
    return beforeMsn;
  }

  public Iterable<Cell<?>> getBeforeCells() {
    return beforeCells;
  }

  public Long getAfterMsn() {
    return afterMsn;
  }

  public Iterable<Cell<?>> getAfterCells() {
    return afterCells;
  }
}

