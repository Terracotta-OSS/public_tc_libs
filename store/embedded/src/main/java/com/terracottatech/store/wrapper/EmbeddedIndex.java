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
package com.terracottatech.store.wrapper;

import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.indexing.Index;

import java.util.function.Function;

import static com.terracottatech.store.indexing.Index.Status.DEAD;
import static com.terracottatech.store.indexing.Index.Status.INITALIZING;
import static com.terracottatech.store.indexing.Index.Status.LIVE;

/**
 *
 * @author cdennis
 */
class EmbeddedIndex<T extends Comparable<T>> implements Index<T> {

  private final Function<SovereignIndexSettings, IndexSettings> settingsConverter;
  private final SovereignIndex<T> index;

  EmbeddedIndex(SovereignIndex<T> index, Function<SovereignIndexSettings, IndexSettings> settingsConverter) {
    this.index = index;
    this.settingsConverter = settingsConverter;
  }

  @Override
  public CellDefinition<T> on() {
    return index.on();
  }

  @Override
  public IndexSettings definition() {
    return settingsConverter.apply(index.definition());
  }

  SovereignIndex<T> getSovereignIndex() {
    return index;
  }

  @Override
  public Status status() {
    switch(index.getState()) {
      case UNKNOWN:
        return DEAD;
      case CREATED:
      case LOADING:
        return INITALIZING;
      case LIVE:
        return LIVE;
      default:
        throw new IllegalStateException("Unknown state: "+index.getState());
    }
  }
}
