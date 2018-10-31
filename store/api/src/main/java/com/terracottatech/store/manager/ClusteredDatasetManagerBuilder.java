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

package com.terracottatech.store.manager;

import com.terracottatech.store.StoreException;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A ClusteredDatasetManagerBuilder allows configuration of the interaction with a cluster.
 */
public interface ClusteredDatasetManagerBuilder {

  /**
   * The default connection timeout in milliseconds.
   */
  long DEFAULT_CONNECTION_TIMEOUT_MS = 20000L;

  /**
   * The default reconnect timeout in milliseconds.  A value of zero
   * indicates no timeout.
   */
  long DEFAULT_RECONNECT_TIMEOUT_MS = 0L;

  /**
   * Creates the DatasetManager that has been configured using this ClusteredDatasetManagerBuilder
   *
   * @return a DatasetManager instance
   * @throws StoreException if the creation of the DatasetManager fails
   */
  DatasetManager build() throws StoreException;

  /**
   * Configures this {@code DatasetManager} to use a specific connection timeout.
   *
   * <p>
   *   The default timeout is given by {@link ClusteredDatasetManagerBuilder#DEFAULT_CONNECTION_TIMEOUT_MS}.
   * </p>
   *
   * @param timeout connection timeout
   * @param unit connection timeout unit
   * @return a {@code ClusteredDatasetManagerBuilder} to allow further configuration
   */
  ClusteredDatasetManagerBuilder withConnectionTimeout(long timeout, TimeUnit unit);

  /**
   * Configures this {@code DatasetManager} to use the specific reconnect timeout.
   * This is used, instead of the {@link #withConnectionTimeout} value when attempting
   * to reconnect to a server following discovery of a dropped connection.
   * <p>
   *   The default timeout is given by {@link ClusteredDatasetManagerBuilder#DEFAULT_RECONNECT_TIMEOUT_MS}.  A
   *   value of zero indicates reconnection attempts do not time out.
   * </p>
   * @param timeout reconnect timeout
   * @param unit reconnect timeout unit
   * @return a {@code ClusteredDatasetManagerBuilder} to allow further configuration
   */
  ClusteredDatasetManagerBuilder withReconnectTimeout(long timeout, TimeUnit unit);

  /**
   * Sets an alias to identify this clustered client. By default, one is randomly generated.
   *
   * @param alias the clustered client's alias
   * @return a {@code ClusteredDatasetManagerBuilder} to allow further configuration
   */
  ClusteredDatasetManagerBuilder withClientAlias(String alias);

  /**
   * Sets some tags to categorize this clustered client. Replaces any previously set tags.
   *
   * @param tags a list of tags
   * @return a {@code ClusteredDatasetManagerBuilder} to allow further configuration
   */
  default ClusteredDatasetManagerBuilder withClientTags(String... tags) {
    return withClientTags(new LinkedHashSet<>(Arrays.asList(tags)));
  }

  /**
   * Sets some tags to categorize this clustered client. Replaces any previously set tags.
   *
   * @param tags a list of tags
   * @return a {@code ClusteredDatasetManagerBuilder} to allow further configuration
   */
  ClusteredDatasetManagerBuilder withClientTags(Set<String> tags);
}
