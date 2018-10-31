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

package com.terracottatech.store.server.indexing;

import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;

public class IndexingUtilities {

  public static SovereignIndexSettings convertSettings(IndexSettings setting) {
    if (setting.equals(IndexSettings.btree())) {
      return SovereignIndexSettings.BTREE;
    } else {
      throw new IllegalArgumentException("Unsupported index setting: " + setting);
    }
  }

  public static IndexSettings convertSettings(SovereignIndexSettings setting) {
    if (setting.equals(SovereignIndexSettings.btree())) {
      return IndexSettings.btree();
    } else {
      throw new IllegalArgumentException("Unsupported index setting: " + setting);
    }
  }

  public static Index.Status convertStatus(SovereignIndex.State state) {
    switch (state) {
      case CREATED:
      case LOADING:
        return Index.Status.INITALIZING;
      case LIVE:
        return Index.Status.LIVE;
      case UNKNOWN:
        return Index.Status.POTENTIAL;
    }
    throw new IllegalArgumentException("Unknown index state: " + state);
  }
}
