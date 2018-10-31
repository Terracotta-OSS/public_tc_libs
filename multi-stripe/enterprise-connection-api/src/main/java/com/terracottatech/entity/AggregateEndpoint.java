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
package com.terracottatech.entity;

import org.terracotta.connection.entity.Entity;

import java.util.List;


public interface AggregateEndpoint<T extends Entity> {
  /**
   * The list of entities in stripe order requested by the EntityAggregatingService.
   * Note: if an entity was not created for the stripe, it will not be included in this
   * list.  As a result the size if this list may not be equal to the number of stripes
   * in the cluster
   *
   * @return list of entities created for this aggregate
   */
  List<T> getEntities();
  /**
   *
   * @return the name of the aggregate entity as well as each underlying stripe entity be convention
   */
  String getName();
  /**
   *
   * @return the version of the aggregate entity as well as each underlying stripe entity be convention
   */
  long getVersion();
  /**
   * close all the underlying stripe entities
   */
  void close();

}
