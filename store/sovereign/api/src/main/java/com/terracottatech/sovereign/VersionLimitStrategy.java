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
package com.terracottatech.sovereign;

import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;

import java.io.Serializable;
import java.util.function.BiFunction;

/**
 * Provides support for limiting {@link com.terracottatech.store.Record Record} versions.
 *
 * @param <Z> the {@link TimeReference} type of records processed by this {@code VersionLimitStrategy}
 *
 * @author Clifford W. Johnson
 */
@SuppressWarnings("serial")
public interface VersionLimitStrategy<Z extends TimeReference<Z>> extends Serializable {

  /**
   * Identifies the possible responses from a ...
   */
  enum Retention {
    /**
     * Indicates the {@code Record} version should be dropped from the returned record and
     * the retrieval scan continued.
     */
    DROP,
    /**
     * Indicates the {@code Record} version should be retained in the returned record and
     * the retrieval scan continued.
     */
    KEEP,
    /**
     * Indicates the {@code Record} version should be retained and the retrieval scan finished.
     */
    FINISH
  }

  /**
   * Gets the {@link TimeReference} type expected by this {@code VersionLimitStrategy} implementation.
   *
   * @return the {@code TimeReference} type for this {@code VersionLimitStrategy}
   */
  Class<Z> type();

  /**
   * Gets a function applied as a filter to a {@link TimeReference}.  The {@code TimeReference} is
   * obtained from a version of a {@link com.terracottatech.store.Record Record} before the {@code Record}
   * is updated or returned.
   * <p>
   * The function returned from this method is used to remove stale versions from a {@code Record}
   * being updated or returned from a {@link SovereignDataset} retrieval method.  The function is called
   * for each version of the {@code Record}, in creation order from oldest to newest, until all
   * versions are processed or a {@link Retention#FINISH FINISH} response is returned.
   * <p>
   * The arguments to the returned function are:
   *  <ul>
   *    <li>{@code now} - a {@code TimeReference} instance representing the <i>current</i> time (obtained
   *    from the {@link TimeReferenceGenerator TimeReferenceGenerator} for
   *    {@code <Z>})</li>
   *    <li>{@code t} - the {@code TimeReference} from the {@code Record} version being examined</li>
   *  </ul>
   * The function returns a {@link Retention} value indicating whether or not the version is
   * retained in the updated or retrieved {@code Record}.
   * <p>
   * The default implementation retains all versions.
   *
   * @return a {@code BiFunction} instance used to filter {@code Record} versions for retention
   */
  default BiFunction<Z, Z, Retention> getTimeReferenceFilter() {
    return (now, t) -> Retention.FINISH;
  }
}
