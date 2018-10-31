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
package com.terracottatech.sovereign.description;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * This represents an opaque {@code Serializable} class describing the configuration of the dataset,
 * it's current indexes, and any other specific-to-this-dataset information.
 *
 * @author cschanck
 */
public interface SovereignDatasetDescription extends Serializable {

  /**
   * Retrieve the index descriptions.
   *
   * @return collection of index descriptions.
   */
  public List<? extends SovereignIndexDescription<?>> getIndexDescriptions();

  /**
   * Get the UUID for this dataset description.
   * @return
   */
  UUID getUUID();

  /**
   * Get the alias for this dataset description.
   *
   * @return Alias. Default is string representation of the UUID.
   */
  String getAlias();


  /**
   * Get the name of the offheap resource to use for this dataset.
   *
   * @return An optional holding the offheap resource name, if one exists, otherwise the empty Optional.
   */
  Optional<String> getOffheapResourceName();
}
