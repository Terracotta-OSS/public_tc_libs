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
package com.terracottatech.store.client.reconnectable;

import org.terracotta.connection.entity.Entity;

/**
 * Specifies methods in common with all reconnectable entities.
 */
// The methods specified here MUST be implemented in AbstractReconnectableEntity as
// required by ReconnectableProxyEntity.
public interface ReconnectableEntity<T extends Entity> extends Entity {

  /**
   * Replaces the current wrapped {@link Entity} with a new instance.
   * @param newEntity the new {@code Entity} to  use
   */
  void swap(T newEntity);

  // Overridden here for ReconnectableProxyEntity.invoke execution support
  @Override
  void close();

  /**
   * Adds an action to take when {@link #close()} is called.
   * @param closeAction the action to take at close
   */
  void onClose(Runnable closeAction);
}
