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

package com.terracottatech.store.common.indexing;

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;

/**
 * A static snapshot of a remote index.
 */
public class ImmutableIndex<T extends Comparable<T>> implements Index<T> {

  private final CellDefinition<T> on;
  private final IndexSettings definition;
  private final Index.Status status;

  public ImmutableIndex(CellDefinition<T> on, IndexSettings definition, Status status) {
    this.on = on;
    this.definition = definition;
    this.status = status;
  }

  @Override
  public CellDefinition<T> on() {
    return on;
  }

  @Override
  public IndexSettings definition() {
    return definition;
  }

  @Override
  public Status status() {
    return status;
  }
}
