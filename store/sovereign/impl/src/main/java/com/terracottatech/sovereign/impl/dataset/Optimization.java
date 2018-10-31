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

package com.terracottatech.sovereign.impl.dataset;

import com.terracottatech.sovereign.impl.compute.CellComparison;

/**
 * Data class containing the results of optimization.
 */
class Optimization<K extends Comparable<K>> {

  private final CellComparison<K> cellComparison;
  private final String normalizedFilterExpression;
  private final boolean skipScan;

  private Optimization(Builder<K> builder) {
    this.cellComparison = builder.cellComparison;
    this.normalizedFilterExpression = builder.normalizedFilterExpression;
    this.skipScan = builder.skipScan;
  }

  /**
   * Given a filterDescription, look for optimization possibilities.
   */
  CellComparison<K> getCellComparison() {
    return cellComparison;
  }

  String getNormalizedFilterExpression() {
    return normalizedFilterExpression;
  }

  boolean doSkipScan() {
    return skipScan;
  }

  static <K extends Comparable<K>> Optimization<K> emptyOptimisation() {
    return Optimization.<K>builder().build();
  }

  static <K extends Comparable<K>> Builder<K> builder() {
    return new Builder<>();
  }

  static class Builder<K extends Comparable<K>> {

    private CellComparison<K> cellComparison;
    private String normalizedFilterExpression;
    private boolean skipScan;

    Builder<K> setCellComparison(CellComparison<K> cellComparison) {
      this.cellComparison = cellComparison;
      return this;
    }

    Builder<K> setNormalizedFilterExpression(String normalizedFilterExpression) {
      this.normalizedFilterExpression = normalizedFilterExpression;
      return this;
    }

    Builder<K> setSkipScan() {
      this.skipScan = true;
      return this;
    }

    Optimization<K> build() {
      return new Optimization<>(this);
    }
  }
}
