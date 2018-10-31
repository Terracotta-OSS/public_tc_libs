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

import java.util.List;
import java.util.Optional;

/**
 * Execution plan for a stream on a {@code SovereignDataset}.
 * <p>
 * A stream pipeline is created when the stream API {@code dataset.records()} is invoked.
 * Clients can use this interface to introspect and understand how the a pipeline will be executed by Sovereign.
 */
public interface StreamPlan {

  /**
   * Whether a sorted cell index will be used to execute the stream.
   *
   * @return true if a sorted cell index will be used
   */
  boolean isSortedCellIndexUsed();

  /**
   * Gets the string equivalent of the known filter(s) expression,
   * that has been examined and processed by the sovereign optimizer for
   * optimization possibilities.
   *
   * @return the filter expression in string form
   */
  String usedFilterExpression();

  /**
   * Gets the normalized filter expression, if the expression was normalized.
   * <p>
   * An expression is normalized, if and only if there are indexed cell usages
   * inside negations and disjunctions.
   *
   * @return normalized filter expression, if normalized, empty otherwise
   */
  Optional<String> getNormalizedFilterExpression();

  /**
   * Count the number of unknown filters in the stream pipeline.
   * <p>
   * Unknown filters are raw filter lambdas of the form {@code ((r) -> (r).get(intCell) < 100)}
   * that does not use the DSL api provided by TC store to express the
   * filtering criteria.
   * <p>
   * Once an unknown filter is detected in the pipeline, no further filters
   * (even known ones) will be used to introspect and find optimization
   * possibilities.
   *
   * @return number of unknown filters
   */
  int unknownFilterCount();

  /**
   * Count the total number of unused but known and transparent filters in the pipeline
   * due to the existence of unknown filters somewhere in the pipeline.
   *
   * @return Number of unused known filters
   */
  int unusedKnownFilterCount();

  /**
   * List of known filter(s) that was not used due to
   * one or more unknown filter(s) appearing in between.
   *
   * @return a list of strings, if present, empty list otherwise
   */
  List<String> unusedKnownFilterExpression();

  /**
   * Gets the total time taken in nanoseconds for pre-execution analysis of the stream pipeline.
   *
   * @return Total nanoseconds to complete the pre-execution analysis
   */
  long planTimeInNanos();

  /**
   * Gets the executing plan details.
   *
   * @return the executing plan
   */
  Plan executingPlan();

  /**
   * Returns a print ready representation of this {@code StreamPlan}.
   *
   * @return print friendly string representation
   */
  String toString();
}
